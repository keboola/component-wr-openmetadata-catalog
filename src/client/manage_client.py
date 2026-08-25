"""Tier-2 Management API client (spec 3.2 / 5.4, research 9.7).

With a ``#manage_token`` (super/application token carrying
``manage:storage-tokens``) this enumerates the organisation's projects and
mints a short-lived Storage token per project — "one credential -> all
projects". This is an org-wide blast-radius credential, so the exact project
list is logged every run and only short-lived tokens are minted; the manage
token is never used to read data.

Each minted token carries ``canManageBuckets`` because that is the only lever
the Keboola token model offers to make *all* of a project's buckets visible to
a freshly minted token (there is no read-only "all buckets" flag). That grant
also permits bucket write/create/delete — over-privileged for a read-only
cataloguer — so its blast radius is bounded by a short ``expiresIn`` and by
withholding file-staging and trash-purge access. See ``mint_storage_token``.

A scope/permission failure raises :class:`ManageScopeError` (a *degrade* signal)
rather than a hard config error, so the orchestrator can fall back to Tier-1.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = frozenset({500, 502, 503, 504})
# A read-only Storage token minted for enumeration should expire quickly.
_MINTED_TOKEN_EXPIRES_SECONDS = 3600


class ManageScopeError(Exception):
    """Enumeration/mint failed on scope or permission — degrade to Tier-1."""


@dataclass
class MintedProject:
    project_id: str
    project_name: str
    storage_token: str
    storage_url: str


class ManageClient:
    """Enumerate org projects and mint per-project read-only Storage tokens."""

    def __init__(
        self,
        host: str,
        manage_token: str,
        *,
        timeout: int = 60,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        self.host = host.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "X-KBC-ManageApiToken": manage_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _request(self, method: str, path: str, json_body: dict | None = None) -> object:
        url = f"{self.host}{path}"
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(method, url, json=json_body, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    time.sleep(self.backoff_base * (2**attempt))
                    continue
                raise ManageScopeError(f"Management API request failed: {method} {path}: {exc}") from exc
            if response.status_code in (401, 403):
                raise ManageScopeError(
                    f"Management API rejected the manage token ({response.status_code}) on {method} {path}. "
                    "The token likely lacks manage:storage-tokens scope."
                )
            if response.status_code in _RETRYABLE_STATUS and attempt < self.max_retries:
                time.sleep(self.backoff_base * (2**attempt))
                continue
            if response.status_code >= 400:
                raise ManageScopeError(
                    f"Management API {method} {path} failed: {response.status_code} {response.text[:300]}"
                )
            return response.json() if response.content else {}
        raise ManageScopeError(f"Management API exhausted retries: {method} {path}")

    def enumerate_projects(self, organization_id: str) -> list[dict]:
        raw = self._request("GET", f"/manage/organizations/{organization_id}/projects")
        projects = raw if isinstance(raw, list) else []
        logger.info(
            "Tier-2 enumerated %d project(s): %s",
            len(projects),
            [f"{p.get('id')}:{p.get('name')}" for p in projects],
        )
        return projects

    def mint_storage_token(self, project_id: str, project_name: str) -> MintedProject:
        # ``canManageBuckets: True`` is required for the cataloguer to see the
        # project's buckets AT ALL. This is a Keboola token-model limitation, not
        # a design choice (see the create-token spec, kbc-manage-api-php-client
        # apiary.apib "Create Storage Token in Project"):
        #   * There is NO read-only "all buckets" flag. The only lever that grants
        #     a freshly minted token visibility of every bucket is
        #     ``canManageBuckets`` ("full permissions on tabular storage").
        #   * The precise-scope alternative, ``bucketPermissions: {<id>: read}``,
        #     needs the bucket IDs enumerated up front — but the Management API has
        #     no bucket-list endpoint, and listing via the Storage API
        #     ``GET /v2/storage/buckets`` returns 0 buckets for a token that lacks
        #     bucket access. So enumerating buckets itself would require a
        #     ``canManageBuckets`` bootstrap token: no net least-privilege gain,
        #     just more moving parts.
        # VERIFIED against the real Management API: a token minted with
        # ``canManageBuckets: False`` yields ``bucketPermissions: {}`` and lists 0
        # buckets, so Tier-2 catalogs nothing. ``canManageBuckets: True`` lists all
        # buckets. MAINTAINER CAVEAT: this also permits bucket create/delete/write
        # — over-privileged for a read-only cataloguer. A tighter read-only mint
        # would need a Keboola feature that does not exist today (a read-all-buckets
        # token flag, or a Management API bucket-list endpoint to drive per-bucket
        # ``read`` grants). The blast radius is bounded by a short ``expiresIn`` and
        # by never granting file-staging or trash-purge access.
        body = {
            "description": "keboola.wr-openmetadata-catalog catalog (auto, short-lived)",
            "canReadAllFileUploads": False,
            "canManageBuckets": True,
            "expiresIn": _MINTED_TOKEN_EXPIRES_SECONDS,
        }
        result = self._request("POST", f"/manage/projects/{project_id}/tokens", json_body=body)
        token = result.get("token") if isinstance(result, dict) else None
        if not token:
            raise ManageScopeError(f"Mint returned no token for project {project_id}")
        return MintedProject(
            project_id=str(project_id),
            project_name=project_name,
            storage_token=token,
            storage_url=self.host,
        )

    def enumerate_and_mint(self, organization_id: str | None) -> list[MintedProject]:
        """Enumerate the ONE configured org's projects (path-scoped) and mint a
        read-only token per project.

        The org id is required (enforced by the Configuration validator); the
        enumerate call hits the path-scoped ``GET /manage/organizations/{id}/projects``
        directly and never the list-all ``GET /manage/organizations``, so the scope
        is bounded to that single org.
        """
        if not organization_id:
            raise ManageScopeError("organization_id is required for Tier-2 enumeration.")
        minted: list[MintedProject] = []
        for project in self.enumerate_projects(str(organization_id)):
            pid = str(project.get("id"))
            pname = project.get("name") or pid
            minted.append(self.mint_storage_token(pid, pname))
        if not minted:
            raise ManageScopeError("No projects enumerated for the organization.")
        return minted
