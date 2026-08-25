"""Keboola Storage + Configurations API reader (research 9.2, spec 1/2).

Reads project metadata (buckets, tables, columns, native/legacy datatypes,
primary keys, descriptions, sharing/linkage), plus the producing component
configs (``storage.input/output`` + transformation blocks/codes/SQL) and
flows/orchestrations needed for the pipeline and lineage passes.

The component only ever *reads*; a read-only Storage token is sufficient. The
caller resolves which credential to use (row ``#storage_token`` / injected
``KBC_TOKEN`` / a Tier-2 minted token) via :func:`resolve_storage_credentials`.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests
from keboola.component.exceptions import UserException

logger = logging.getLogger(__name__)

_BRANCH_METADATA_KEY = "KBC.createdBy.branch.id"
_DESCRIPTION_KEY = "KBC.description"
_RETRYABLE_STATUS = frozenset({500, 502, 503, 504})


def resolve_storage_credentials(
    *,
    row_token: str | None,
    injected_token: str | None,
    injected_url: str | None,
    minted_token: str | None = None,
    minted_url: str | None = None,
) -> tuple[str, str]:
    """Resolve ``(token, base_url)`` for a project (spec 3.2).

    Priority: a Tier-2 minted token, else the row ``#storage_token``, else the
    injected ``KBC_TOKEN``/``KBC_URL`` (``forward_token``). Raises
    ``UserException`` when no usable credential exists.
    """
    if minted_token and minted_url:
        return minted_token, minted_url.rstrip("/")
    if row_token:
        url = injected_url or "https://connection.keboola.com"
        return row_token, url.rstrip("/")
    if injected_token and injected_url:
        return injected_token, injected_url.rstrip("/")
    raise UserException(
        "No Storage credential available: provide a row #storage_token, or enable forward_token "
        "for the host project, or configure a Tier-2 #manage_token."
    )


@dataclass
class SourceColumn:
    name: str
    definition: dict | None = None
    basetype: str | None = None
    legacy: dict = field(default_factory=dict)
    description: str | None = None


@dataclass
class SourceTable:
    id: str
    name: str
    display_name: str | None = None
    description: str | None = None
    primary_key: list[str] = field(default_factory=list)
    columns: list[SourceColumn] = field(default_factory=list)
    row_count: int | None = None
    data_size_bytes: int | None = None
    last_import_date: str | None = None
    is_alias: bool = False
    source_table: dict | None = None
    branch_id: str | None = None


@dataclass
class SourceBucket:
    id: str
    name: str
    stage: str | None = None
    path: str | None = None
    display_name: str | None = None
    description: str | None = None
    sharing: str | None = None
    has_external_schema: bool = False
    source_bucket: dict | None = None
    backend: str | None = None
    branch_id: str | None = None


def _metadata_value(entries: list | None, key: str) -> str | None:
    for entry in entries or []:
        if isinstance(entry, dict) and entry.get("key") == key:
            return entry.get("value")
    return None


class StorageReader:
    """Read-only Keboola Storage + Configurations client."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        production_only: bool = True,
        timeout: int = 60,
        max_retries: int = 4,
        backoff_base: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.production_only = production_only
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.session = session or requests.Session()
        self.session.headers.update({"X-StorageApi-Token": token, "Accept": "application/json"})
        self._branch_id: str | None = None

    # ------------------------------------------------------------------ core

    def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{self.base_url}{path}"
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    time.sleep(self.backoff_base * (2**attempt))
                    continue
                raise UserException(f"Keboola Storage request failed: GET {path}: {exc}") from exc
            if response.status_code in (401, 403):
                raise UserException(
                    f"Keboola Storage rejected the token ({response.status_code}) on GET {path}. "
                    "Check the Storage token is valid and has read access."
                )
            if response.status_code in _RETRYABLE_STATUS and attempt < self.max_retries:
                time.sleep(self.backoff_base * (2**attempt))
                continue
            if response.status_code >= 400:
                raise UserException(f"Keboola Storage GET {path} failed: {response.status_code}")
            return response.json()
        raise UserException(f"Keboola Storage request exhausted retries: GET {path}")

    def read_snapshot_rows(self, table_id: str, *, limit: int = 1_000_000) -> list[dict]:
        """Best-effort read of a prior snapshot table via data-preview (CSV).

        Returns ``[]`` if the table does not exist yet (first run) or on any
        read error — the merge base is then rebuilt on the next full refresh.

        Limitation: ``data-preview`` is a *row-capped* endpoint that caps rows
        server-side, *below* the requested ``limit``, so a hit on the requested
        ``limit`` never fires. For very large catalogs (5k+ tables) it can return
        fewer rows than the snapshot actually holds. Truncation is therefore
        detected against the table's own server-side ``rowsCount`` (not the
        requested ``limit``): when fewer rows come back than the table reports, the
        merge base is truncated, so changed fields on entities beyond the cap
        silently stop propagating (those entities fail *safe* to
        ``skipped_diverged`` and are never clobbered). A truncated read emits a
        ``logger.warning``; the durable fix is an async full-table export, which is
        out of scope here.
        """
        url = f"{self.base_url}/v2/storage/tables/{table_id}/data-preview"
        try:
            response = self.session.get(url, params={"limit": limit}, timeout=self.timeout)
            if response.status_code >= 400:
                return []
            rows = list(csv.DictReader(io.StringIO(response.text)))
        except requests.RequestException, csv.Error:
            return []
        total = self._table_rows_count(table_id)
        if total is not None and len(rows) < total:
            logger.warning(
                "Snapshot merge base for '%s' is truncated: data-preview returned %d of %d rows "
                "(server-side row cap); updates to existing entities beyond the cap may be skipped "
                "for this run. This affects large catalogs (5k+ tables).",
                table_id,
                len(rows),
                total,
            )
        return rows

    def _table_rows_count(self, table_id: str) -> int | None:
        """Best-effort server-side ``rowsCount`` for a table (truncation detection).

        Returns ``None`` on any read error so snapshot reading never fails just
        because the row count could not be fetched.
        """
        try:
            response = self.session.get(f"{self.base_url}/v2/storage/tables/{table_id}", timeout=self.timeout)
            if response.status_code >= 400:
                return None
            data = response.json()
        except requests.RequestException, ValueError:
            return None
        count = data.get("rowsCount") if isinstance(data, dict) else None
        if isinstance(count, bool):  # bool subclasses int; a JSON boolean is not a row count
            return None
        if isinstance(count, int):
            return count
        if isinstance(count, str) and count.isdigit():
            return int(count)
        return None

    # ------------------------------------------------------------ connection

    def verify_token(self) -> dict:
        return self._get("/v2/storage/tokens/verify")

    def test_connection(self) -> bool:
        try:
            data = self._get("/v2/storage")
            return isinstance(data, dict) and "api" in data
        except UserException:
            raise
        except Exception as exc:  # noqa: BLE001 - reported as a boolean for sync action
            logger.warning("Storage connection test failed: %s", exc)
            return False

    @property
    def branch_id(self) -> str | None:
        if self._branch_id is None:
            self._branch_id = self._resolve_main_branch_id()
        return self._branch_id

    def _resolve_main_branch_id(self) -> str | None:
        try:
            branches = self._get("/v2/storage/dev-branches")
        except UserException:
            return None
        for branch in branches if isinstance(branches, list) else []:
            if branch.get("isDefault"):
                return str(branch.get("id"))
        return None

    # --------------------------------------------------------------- buckets

    def _passes_branch_filter(self, branch_id: str | None) -> bool:
        if not self.production_only:
            return True
        return branch_id is None

    def list_buckets(self) -> list[SourceBucket]:
        raw = self._get("/v2/storage/buckets", params={"include": "metadata"})
        buckets: list[SourceBucket] = []
        for item in raw if isinstance(raw, list) else []:
            branch = _metadata_value(item.get("metadata"), _BRANCH_METADATA_KEY)
            if not self._passes_branch_filter(branch):
                continue
            buckets.append(
                SourceBucket(
                    id=item["id"],
                    name=item.get("name", item["id"]),
                    stage=item.get("stage"),
                    path=item.get("path") or item.get("name"),
                    display_name=item.get("displayName"),
                    description=_metadata_value(item.get("metadata"), _DESCRIPTION_KEY) or item.get("description"),
                    sharing=item.get("sharing"),
                    has_external_schema=bool(item.get("hasExternalSchema")),
                    source_bucket=item.get("sourceBucket"),
                    backend=item.get("backend"),
                    branch_id=branch,
                )
            )
        return buckets

    def iter_tables(self, bucket_id: str) -> Iterator[SourceTable]:
        summaries = self._get(f"/v2/storage/buckets/{bucket_id}/tables", params={"include": "metadata"})
        for summary in summaries if isinstance(summaries, list) else []:
            yield self.get_table(summary["id"])

    def get_table(self, table_id: str) -> SourceTable:
        raw = self._get(
            f"/v2/storage/tables/{table_id}",
            params={"include": "columns,metadata,columnMetadata,buckets"},
        )
        return self._parse_table(raw)

    def _parse_table(self, raw: dict) -> SourceTable:
        column_metadata: dict[str, list] = raw.get("columnMetadata") or {}
        typed_defs = self._typed_column_definitions(raw)
        columns: list[SourceColumn] = []
        for name in raw.get("columns") or []:
            legacy_entries = column_metadata.get(name) or []
            legacy = {
                "type": _metadata_value(legacy_entries, "KBC.datatype.type"),
                "basetype": _metadata_value(legacy_entries, "KBC.datatype.basetype"),
                "length": _metadata_value(legacy_entries, "KBC.datatype.length"),
            }
            typed = typed_defs.get(name)
            columns.append(
                SourceColumn(
                    name=name,
                    definition=(typed or {}).get("definition"),
                    basetype=(typed or {}).get("basetype"),
                    legacy={k: v for k, v in legacy.items() if v is not None},
                    description=_metadata_value(legacy_entries, _DESCRIPTION_KEY),
                )
            )

        definition = raw.get("definition") or {}
        primary_key = raw.get("primaryKey") or definition.get("primaryKeysNames") or []
        branch = _metadata_value(raw.get("metadata"), _BRANCH_METADATA_KEY)
        return SourceTable(
            id=raw["id"],
            name=raw.get("name", raw["id"]),
            display_name=raw.get("displayName"),
            description=_metadata_value(raw.get("metadata"), _DESCRIPTION_KEY) or raw.get("description"),
            primary_key=list(primary_key),
            columns=columns,
            row_count=raw.get("rowsCount"),
            data_size_bytes=raw.get("dataSizeBytes"),
            last_import_date=raw.get("lastImportDate"),
            is_alias=bool(raw.get("isAlias")),
            source_table=raw.get("sourceTable"),
            branch_id=branch,
        )

    @staticmethod
    def _typed_column_definitions(raw: dict) -> dict[str, dict]:
        """Map column name -> {'definition': {...}, 'basetype': ...} from typed ``definition.columns``."""
        result: dict[str, dict] = {}
        for col in (raw.get("definition") or {}).get("columns") or []:
            name = col.get("name")
            if name:
                result[name] = {"definition": col.get("definition"), "basetype": col.get("basetype")}
        return result

    # ------------------------------------------------ configs / flows (E13-17)

    def list_component_configs(self) -> list[dict]:
        """Return every component with its configurations (params, storage, rows).

        Used for the pipeline pass (E13/E14) and declared/column lineage
        (E16/E17). Flows/orchestrations arrive here too, distinguished by
        ``componentId`` downstream.
        """
        branch = self.branch_id or "default"
        raw = self._get(
            f"/v2/storage/branch/{branch}/components",
            params={"include": "configuration,rows"},
        )
        return raw if isinstance(raw, list) else []

    def get_component_config(self, component_id: str, config_id: str) -> dict:
        branch = self.branch_id or "default"
        return self._get(f"/v2/storage/branch/{branch}/components/{component_id}/configs/{config_id}")
