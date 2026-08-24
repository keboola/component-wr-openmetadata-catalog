"""Hand-rolled thin OpenMetadata REST client (spec 3.3 / 3.5).

No OM SDK: the ``openmetadata-ingestion`` package version-locks to the server
and constrains the Python floor, so a single image could target only one OM
version. This client targets the guaranteed **stable 1.13.4** surface and gates
the three 2.0-only niceties (``deleteStale``, bulk ``?overrideMetadata=``,
delete-lineage-by-source-name) behind a ``GET /system/version`` probe, with
1.13.4 fallbacks.

Entity bodies are plain dicts (built by ``mapping.*``); this client only knows
how to authenticate, route, retry and paginate.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import requests
from keboola.component.exceptions import UserException

logger = logging.getLogger(__name__)

# Entity kinds whose collection lives under ``/services/``.
_SERVICE_KINDS = frozenset({"databaseServices", "pipelineServices"})

_RETRYABLE_STATUS = frozenset({500, 502, 503, 504})
_JSON_PATCH_CT = "application/json-patch+json"


class OMClientError(Exception):
    """Unexpected OM client / server error (maps to app error, exit 2)."""


class OMAuthError(UserException):
    """OM auth failure (401/403) — user-fixable, exit 1."""


class OMNotFound(OMClientError):
    """Requested entity does not exist (404)."""


class OMPreconditionFailed(OMClientError):
    """A PATCH failed with 412 — caller should refetch and retry once."""


def _version_tuple(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for chunk in version.split("-")[0].split("."):
        try:
            parts.append(int(chunk))
        except ValueError:
            break
    return tuple(parts)


class OMClient:
    """Thin OpenMetadata REST client."""

    def __init__(
        self,
        host: str,
        token: str,
        *,
        verify_ssl: bool = True,
        timeout: int = 60,
        max_retries: int = 4,
        backoff_base: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = f"{host.rstrip('/')}/api/v1"
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.server_version: str | None = None
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.session.verify = verify_ssl

    # ------------------------------------------------------------------ core

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: dict | None = None,
        content_type: str | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        headers: dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = content_type

        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    json=json_body,
                    params=params,
                    headers=headers or None,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    self._sleep(attempt)
                    continue
                raise OMClientError(f"OM request failed after retries: {method} {url}: {exc}") from exc

            if response.status_code in (401, 403):
                raise OMAuthError(
                    f"OpenMetadata rejected the bot token ({response.status_code}) on {method} {path}. "
                    "Check that #bot_token is valid and the bot has the required policy."
                )
            if response.status_code == 404:
                raise OMNotFound(f"Not found: {method} {path}")
            if response.status_code == 412:
                raise OMPreconditionFailed(f"Precondition failed (412) on {method} {path}")
            if response.status_code in _RETRYABLE_STATUS and attempt < self.max_retries:
                logger.warning("OM %s %s -> %s, retrying", method, path, response.status_code)
                self._sleep(attempt)
                continue
            if response.status_code >= 400:
                raise OMClientError(f"OM {method} {path} failed: {response.status_code} {response.text[:500]}")
            return response

        raise OMClientError(f"OM request exhausted retries: {method} {url}: {last_exc}")

    def _sleep(self, attempt: int) -> None:
        time.sleep(self.backoff_base * (2**attempt))

    @staticmethod
    def _path_for(kind: str, suffix: str = "") -> str:
        base = f"/services/{kind}" if kind in _SERVICE_KINDS else f"/{kind}"
        return f"{base}{suffix}"

    # -------------------------------------------------------------- version

    def probe_version(self) -> dict:
        """GET /system/version -> {version, revision, timestamp}. Records it."""
        response = self._request("GET", "/system/version")
        data = response.json()
        self.server_version = data.get("version")
        logger.info("OpenMetadata server version: %s", self.server_version)
        return data

    @property
    def is_2_0_or_newer(self) -> bool:
        if not self.server_version:
            return False
        return _version_tuple(self.server_version) >= (2, 0)

    def verify_auth(self) -> dict:
        """Authenticated ping: GET /users/loggedInUser -> the bot's own user.

        ``GET /system/version`` is in OM's ``JwtFilter.EXCLUDED_ENDPOINTS`` (spec
        3.5), so it proves reachability but NOT that the bot token is valid — a
        bad/expired ``#bot_token`` still gets a 200 there. This endpoint is behind
        the JWT filter, so an invalid token surfaces as ``OMAuthError`` (401/403),
        which ``testConnection`` turns into a clear ``UserException``.
        """
        response = self._request("GET", "/users/loggedInUser")
        return response.json()

    # --------------------------------------------------------------- writes

    def put_entity(self, kind: str, body: dict) -> dict:
        """createOrUpdate a single entity (databases/schemas/tables/pipelines/services)."""
        response = self._request("PUT", self._path_for(kind), json_body=body)
        return response.json()

    def bulk_put_tables(self, bodies: list[dict], *, override_metadata: bool = False) -> dict:
        """PUT /tables/bulk. ``?overrideMetadata=`` is 2.0-only and gated."""
        params: dict | None = None
        if override_metadata:
            if not self.is_2_0_or_newer:
                logger.debug("overrideMetadata requested but server < 2.0; ignoring (1.13.4 fallback)")
            else:
                params = {"overrideMetadata": "true"}
        response = self._request("PUT", "/tables/bulk", json_body=bodies, params=params)
        return response.json() if response.content else {}

    def put_pipeline_status(self, pipeline_fqn: str, body: dict) -> dict:
        response = self._request("PUT", f"/pipelines/{quote(pipeline_fqn, safe='')}/status", json_body=body)
        return response.json()

    def put_lineage(self, edge: dict) -> dict:
        response = self._request("PUT", "/lineage", json_body=edge)
        return response.json() if response.content else {}

    def patch_entity(self, kind: str, fqn: str, json_patch: list[dict]) -> dict:
        """Apply an RFC-6902 JSON Patch (touch only owned fields). 412 -> OMPreconditionFailed."""
        path = self._path_for(kind, f"/name/{quote(fqn, safe='')}")
        response = self._request("PATCH", path, json_body=json_patch, content_type=_JSON_PATCH_CT)
        return response.json()

    # ---------------------------------------------------------------- reads

    def get_by_fqn(self, kind: str, fqn: str, *, fields: str | None = None) -> dict | None:
        params = {"fields": fields} if fields else None
        try:
            response = self._request("GET", self._path_for(kind, f"/name/{quote(fqn, safe='')}"), params=params)
        except OMNotFound:
            return None
        return response.json()

    def list_entities(self, kind: str, params: dict | None = None, *, page_size: int = 200) -> Iterator[dict]:
        """Cursor-paginated iteration over a collection (spec 4.4)."""
        query = dict(params or {})
        query["limit"] = page_size
        after: str | None = None
        while True:
            if after:
                query["after"] = after
            response = self._request("GET", self._path_for(kind), params=query)
            payload = response.json()
            yield from payload.get("data", [])
            after = (payload.get("paging") or {}).get("after")
            if not after:
                break

    # ------------------------------------------------------------- deletes

    def soft_delete(self, kind: str, entity_id: str, *, recursive: bool = True) -> None:
        params = {"hardDelete": "false", "recursive": "true" if recursive else "false"}
        self._request("DELETE", self._path_for(kind, f"/{entity_id}"), params=params)

    def delete_lineage_by_source(self, entity_type: str, fqn: str, source: str) -> None:
        """Drop our edges of one ``source`` from an entity.

        2.0+: by-name convenience endpoint. 1.13.4: resolve the id then use the
        by-id form (spec 3.5). The capability exists on both; only the path differs.
        """
        if self.is_2_0_or_newer:
            path = f"/lineage/source/name/{entity_type}/{quote(fqn, safe='')}/type/{quote(source, safe='')}"
            self._request("DELETE", path)
            return
        entity_id = self._resolve_entity_id(entity_type, fqn)
        if entity_id is None:
            logger.debug("delete_lineage_by_source: entity %s not found, skipping", fqn)
            return
        self._request("DELETE", f"/lineage/{entity_type}/{entity_id}/type/{quote(source, safe='')}")

    def delete_stale(self, body: dict) -> dict:
        """DELETE /tables/deleteStale (2.0-only). Caller must gate on version."""
        if not self.is_2_0_or_newer:
            raise OMClientError("deleteStale is 2.0-only; use the self-diff fallback on 1.13.4")
        response = self._request("DELETE", "/tables/deleteStale", json_body=body)
        return response.json() if response.content else {}

    # ------------------------------------------------------------- helpers

    def _resolve_entity_id(self, entity_type: str, fqn: str) -> str | None:
        kind = f"{entity_type}s" if not entity_type.endswith("s") else entity_type
        entity = self.get_by_fqn(kind, fqn)
        return entity.get("id") if entity else None
