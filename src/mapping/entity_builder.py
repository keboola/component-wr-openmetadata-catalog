"""Catalog entity body builder (E1-E12, spec 2.1 / 4, T10).

Builds the OpenMetadata ``createOrUpdate`` bodies (plain dicts, no OM SDK) for
the DatabaseService, Database, DatabaseSchema and Table entities from parsed
Keboola Storage objects. Deep links (``sourceUrl``) are derived from the stack
UI base + project/bucket/table ids — never a hardcoded stack URL.

Structured metadata (Storage ids, sizing/import facts, sharing/backend, a
public deep link) is additionally written as OpenMetadata **custom
properties** in each entity's ``extension`` — the same typed-field treatment
``mapping.dashboard_builder`` uses for data-app Dashboards, generalised here to
Table/DatabaseSchema/Database. A native ``owners`` entry is set only when a
creator e-mail is *actually present* in the Storage metadata the reader
fetches (``KBC.createdBy.*``) — verified today to hold only a component/config
id, never an e-mail, so Table/DatabaseSchema owners are omitted in practice;
Database (Project) never gets an owner (no such metadata exists at that
level).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from client.storage_reader import SourceBucket, SourceColumn, SourceTable
from mapping import enrichment, fqn
from mapping.datatype import map_datatype
from mapping.table_type import VIEW, detect_table_type

logger = logging.getLogger(__name__)

_SERVICE_TYPE = "CustomDatabase"

# Custom properties defined on the OpenMetadata Table/DatabaseSchema/Database
# types: (name, field type, description). om_types used across this component
# are only ever "string", "email", "hyperlink-cp".
TABLE_CUSTOM_PROPERTIES = [
    ("kbcTableId", "string", "Keboola Storage table id"),
    ("kbcBucketId", "string", "Keboola Storage bucket id"),
    ("kbcStage", "string", "Keboola Storage bucket stage (in/out/sys)"),
    ("kbcRowsCount", "string", "Row count as of the last catalog sync"),
    ("kbcDataSizeBytes", "string", "Table data size in bytes as of the last catalog sync"),
    ("kbcLastImport", "string", "Last import date"),
    ("kbcIsAlias", "string", "Whether the table is an alias (linked/shared) table"),
    ("kbcTableUrl", "hyperlink-cp", "Keboola Storage table URL"),
    ("kbcSyncedAt", "string", "Catalog snapshot time (UTC) — the fields above are as of this time"),
]

SCHEMA_CUSTOM_PROPERTIES = [
    ("kbcBucketId", "string", "Keboola Storage bucket id"),
    ("kbcStage", "string", "Keboola Storage bucket stage (in/out/sys)"),
    ("kbcBackend", "string", "Bucket backend (e.g. snowflake, bigquery)"),
    ("kbcSharing", "string", "Bucket sharing mode, when shared"),
    ("kbcBucketUrl", "hyperlink-cp", "Keboola Storage bucket URL"),
    ("kbcSyncedAt", "string", "Catalog snapshot time (UTC) — the fields above are as of this time"),
]

DATABASE_CUSTOM_PROPERTIES = [
    ("kbcProjectId", "string", "Keboola project id"),
    ("kbcProjectUrl", "hyperlink-cp", "Keboola project storage URL"),
    ("kbcSyncedAt", "string", "Catalog snapshot time (UTC) — the fields above are as of this time"),
]


@dataclass
class BuiltTable:
    """A Table body plus the source FQN for a pending ViewLineage edge (if any)."""

    body: dict
    fqn: str
    table_type: str
    view_source_fqn: str | None = None


class EntityBuilder:
    """Builds catalog entity bodies for one project."""

    def __init__(
        self, service_name: str, project: str, project_id: str | None, ui_base: str, stack_id: str | None = None
    ) -> None:
        self.service_name = service_name
        self.project = project
        self.project_id = project_id or "unknown"
        self.ui_base = ui_base.rstrip("/")
        self.stack_id = stack_id

    # --------------------------------------------------------------- helpers

    def _project_url(self) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/storage"

    def _bucket_url(self, bucket_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/storage/{bucket_id}"

    def _table_url(self, bucket_id: str, table_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/storage/{bucket_id}/table/{table_id}"

    def _public_base(self) -> str:
        """Public Keboola connection base URL (``KBC_STACKID``, falls back to ``ui_base``)."""
        return enrichment.connection_base(self.stack_id, self.ui_base)

    def _kbc_project_url(self) -> str:
        return f"{self._public_base()}/admin/projects/{self.project_id}/storage"

    def _kbc_bucket_url(self, bucket_id: str) -> str:
        return f"{self._public_base()}/admin/projects/{self.project_id}/storage/{bucket_id}"

    def _kbc_table_url(self, bucket_id: str, table_id: str) -> str:
        return f"{self._public_base()}/admin/projects/{self.project_id}/storage/{bucket_id}/table/{table_id}"

    @staticmethod
    def _drop_none(body: dict) -> dict:
        return {k: v for k, v in body.items() if v is not None}

    @staticmethod
    def _creator_email(created_by_metadata: dict[str, str]) -> str | None:
        """Owner e-mail from Storage ``KBC.createdBy.*`` metadata, if any value is e-mail-shaped.

        Verified system metadata (``KBC.createdBy.component.id`` /
        ``KBC.createdBy.configuration.id`` / ``KBC.createdBy.branch.id``)
        records which COMPONENT/CONFIG created the object, never a user
        e-mail — so this returns ``None`` for tables/buckets today. It stays a
        real check (not a hardcoded omission) so a future Storage API
        addition of an e-mail-shaped creator key is picked up without a code
        change; until then the owner is omitted, never fabricated.
        """
        for value in created_by_metadata.values():
            email = enrichment.extract_email(value)
            if email:
                return email
        return None

    def _owners_from_metadata(
        self, created_by_metadata: dict[str, str], owner_resolver: Callable[[str], str | None] | None
    ) -> list[dict] | None:
        return enrichment.native_owners(self._creator_email(created_by_metadata), owner_resolver)

    # ---------------------------------------------------------------- E1-E3

    def database_service_body(self) -> dict:
        return {
            "name": fqn.sanitize_name(self.service_name),
            "serviceType": _SERVICE_TYPE,
            "description": "Keboola Connection stack catalogued by keboola.wr-openmetadata-catalog.",
        }

    def _database_extension(self, available: set[str] | None, synced_at: str | None) -> dict | None:
        values = {
            "kbcProjectId": self.project_id,
            "kbcProjectUrl": enrichment.hyperlink(self._kbc_project_url(), "Open project"),
            "kbcSyncedAt": synced_at,
        }
        extension = {k: v for k, v in values.items() if v is not None and (available is None or k in available)}
        return extension or None

    def database_body(
        self,
        display_name: str | None = None,
        *,
        available: set[str] | None = None,
        synced_at: str | None = None,
    ) -> dict:
        # Database (Project) never gets a native owner — Storage exposes no
        # project-level creator metadata to check.
        return self._drop_none(
            {
                "name": fqn.sanitize_name(self.project),
                "displayName": fqn.sanitize_display_name(display_name or self.project),
                "service": fqn.database_service_fqn(self.service_name),
                "sourceUrl": self._project_url(),
                "extension": self._database_extension(available, synced_at),
            }
        )

    def _schema_extension(self, bucket: SourceBucket, available: set[str] | None, synced_at: str | None) -> dict | None:
        values = {
            "kbcBucketId": bucket.id,
            "kbcStage": bucket.stage,
            "kbcBackend": bucket.backend,
            "kbcSharing": bucket.sharing,
            "kbcBucketUrl": enrichment.hyperlink(self._kbc_bucket_url(bucket.id), "Open bucket"),
            "kbcSyncedAt": synced_at,
        }
        extension = {k: v for k, v in values.items() if v is not None and (available is None or k in available)}
        return extension or None

    def schema_body(
        self,
        bucket: SourceBucket,
        *,
        available: set[str] | None = None,
        synced_at: str | None = None,
        owner_resolver: Callable[[str], str | None] | None = None,
    ) -> dict:
        bucket_path = bucket.path or bucket.name
        return self._drop_none(
            {
                "name": fqn.sanitize_name(bucket_path),
                "displayName": fqn.sanitize_display_name(bucket.display_name or bucket.name),
                "description": fqn.sanitize_display_name(bucket.description),
                "database": fqn.database_fqn(self.service_name, self.project),
                "sourceUrl": self._bucket_url(bucket.id),
                "extension": self._schema_extension(bucket, available, synced_at),
                "owners": self._owners_from_metadata(bucket.created_by_metadata, owner_resolver),
            }
        )

    # ------------------------------------------------------------- E4-E9,E11

    def _column_body(self, column: SourceColumn, ordinal: int) -> dict:
        mapped = map_datatype(
            definition=column.definition,
            basetype=column.basetype,
            legacy=column.legacy,
        )
        body = {
            "name": fqn.sanitize_name(column.name),
            "displayName": fqn.sanitize_display_name(column.name),
            "dataType": mapped.data_type,
            "dataTypeDisplay": mapped.data_type_display,
            "dataLength": mapped.data_length,
            "arrayDataType": mapped.array_data_type,
            "description": fqn.sanitize_display_name(column.description),
            "ordinalPosition": ordinal,
        }
        return self._drop_none(body)

    @staticmethod
    def _disambiguate_column_names(columns: list[dict], table_name: str) -> list[dict]:
        """Ensure the emitted ``columns[]`` carry unique ``name`` values.

        Two distinct source columns can sanitise to the same FQN-safe segment
        (e.g. a dotted ``a.b`` and an underscored ``a_b`` both -> ``a_b``).
        OpenMetadata rejects a table whose ``columns[]`` repeat a name
        (``400 Column name <x> is repeated``), which under the default
        ``collect_and_fail`` mode fails the whole catalog write. Rather than
        drop a column, colliding occurrences after the first get an ordinal
        suffix (``name``, ``name_2``, ``name_3``, ...), so both survive. Only
        ``name`` is rewritten; ``displayName``/``dataTypeDisplay`` (the original
        source name and rendered type) are left untouched. Deterministic:
        stable input order -> stable suffixes. Columns without a collision are
        not touched.
        """
        seen: set[str] = set()
        for col in columns:
            base = col["name"]
            if base not in seen:
                seen.add(base)
                continue
            ordinal = 2
            candidate = f"{base}_{ordinal}"
            while candidate in seen:
                ordinal += 1
                candidate = f"{base}_{ordinal}"
            seen.add(candidate)
            logger.warning(
                "Table %r column name %r collides after sanitisation; disambiguating to %r.",
                table_name,
                base,
                candidate,
            )
            col["name"] = candidate
        return columns

    def _table_extension(
        self,
        bucket: SourceBucket,
        table: SourceTable,
        available: set[str] | None,
        synced_at: str | None,
    ) -> dict | None:
        values = {
            "kbcTableId": table.id,
            "kbcBucketId": bucket.id,
            "kbcStage": bucket.stage,
            "kbcRowsCount": str(table.row_count) if table.row_count is not None else None,
            "kbcDataSizeBytes": str(table.data_size_bytes) if table.data_size_bytes is not None else None,
            "kbcLastImport": table.last_import_date,
            "kbcIsAlias": "true" if table.is_alias else "false",
            "kbcTableUrl": enrichment.hyperlink(self._kbc_table_url(bucket.id, table.id), "Open table"),
            "kbcSyncedAt": synced_at,
        }
        extension = {k: v for k, v in values.items() if v is not None and (available is None or k in available)}
        return extension or None

    def table_body(
        self,
        bucket: SourceBucket,
        table: SourceTable,
        *,
        available: set[str] | None = None,
        synced_at: str | None = None,
        owner_resolver: Callable[[str], str | None] | None = None,
    ) -> BuiltTable:
        bucket_path = bucket.path or bucket.name
        table_type = detect_table_type(
            stage=bucket.stage,
            sharing=bucket.sharing,
            has_external_schema=bucket.has_external_schema,
            has_source_bucket=bucket.source_bucket is not None,
            is_alias=table.is_alias,
        )
        table_fqn = fqn.table_fqn(self.service_name, self.project, bucket_path, table.name)
        columns = [self._column_body(col, i + 1) for i, col in enumerate(table.columns)]
        columns = self._disambiguate_column_names(columns, table.name)

        body: dict = {
            "name": fqn.sanitize_name(table.name),
            "displayName": fqn.sanitize_display_name(table.display_name or table.name),
            "description": fqn.sanitize_display_name(table.description),
            "tableType": table_type,
            "columns": columns,
            "databaseSchema": fqn.schema_fqn(self.service_name, self.project, bucket_path),
            "sourceUrl": self._table_url(bucket.id, table.id),
            "extension": self._table_extension(bucket, table, available, synced_at),
            "owners": self._owners_from_metadata(table.created_by_metadata, owner_resolver),
        }
        if table.primary_key:
            body["tableConstraints"] = [{"constraintType": "PRIMARY_KEY", "columns": list(table.primary_key)}]

        view_source_fqn: str | None = None
        if table_type == VIEW:
            view_source_fqn = self._view_source_fqn(table)
            if view_source_fqn:
                body["schemaDefinition"] = f"CREATE VIEW {table_fqn} AS SELECT * FROM {view_source_fqn}"

        return BuiltTable(
            body=self._drop_none(body),
            fqn=table_fqn,
            table_type=table_type,
            view_source_fqn=view_source_fqn,
        )

    def _view_source_fqn(self, table: SourceTable) -> str | None:
        """FQN of a linked/shared table's source, resolved onto its owning project."""
        source = table.source_table or {}
        source_id = source.get("id")
        if not source_id:
            return None
        parts = str(source_id).split(".")
        if len(parts) < 3:
            return None
        source_bucket_path = ".".join(parts[:2])
        source_table_name = ".".join(parts[2:])
        project_info = source.get("project") or {}
        source_project = project_info.get("name") or self.project
        return fqn.table_fqn(self.service_name, source_project, source_bucket_path, source_table_name)
