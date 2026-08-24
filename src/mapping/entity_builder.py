"""Catalog entity body builder (E1-E12, spec 2.1 / 4, T10).

Builds the OpenMetadata ``createOrUpdate`` bodies (plain dicts, no OM SDK) for
the DatabaseService, Database, DatabaseSchema and Table entities from parsed
Keboola Storage objects. Deep links (``sourceUrl``) are derived from the stack
UI base + project/bucket/table ids — never a hardcoded stack URL.
"""

from __future__ import annotations

from dataclasses import dataclass

from client.storage_reader import SourceBucket, SourceColumn, SourceTable
from mapping import fqn
from mapping.datatype import map_datatype
from mapping.table_type import VIEW, detect_table_type

_SERVICE_TYPE = "CustomDatabase"


@dataclass
class BuiltTable:
    """A Table body plus the source FQN for a pending ViewLineage edge (if any)."""

    body: dict
    fqn: str
    table_type: str
    view_source_fqn: str | None = None


class EntityBuilder:
    """Builds catalog entity bodies for one project."""

    def __init__(self, service_name: str, project: str, project_id: str | None, ui_base: str) -> None:
        self.service_name = service_name
        self.project = project
        self.project_id = project_id or "unknown"
        self.ui_base = ui_base.rstrip("/")

    # --------------------------------------------------------------- helpers

    def _project_url(self) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/storage"

    def _bucket_url(self, bucket_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/storage/{bucket_id}"

    def _table_url(self, bucket_id: str, table_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/storage/{bucket_id}/table/{table_id}"

    @staticmethod
    def _drop_none(body: dict) -> dict:
        return {k: v for k, v in body.items() if v is not None}

    # ---------------------------------------------------------------- E1-E3

    def database_service_body(self) -> dict:
        return {
            "name": fqn.sanitize_name(self.service_name),
            "serviceType": _SERVICE_TYPE,
            "description": "Keboola Connection stack catalogued by keboola.wr-openmetadata-catalog.",
        }

    def database_body(self, display_name: str | None = None) -> dict:
        return self._drop_none(
            {
                "name": fqn.sanitize_name(self.project),
                "displayName": fqn.sanitize_display_name(display_name or self.project),
                "service": fqn.database_service_fqn(self.service_name),
                "sourceUrl": self._project_url(),
            }
        )

    def schema_body(self, bucket: SourceBucket) -> dict:
        bucket_path = bucket.path or bucket.name
        return self._drop_none(
            {
                "name": fqn.sanitize_name(bucket_path),
                "displayName": fqn.sanitize_display_name(bucket.display_name or bucket.name),
                "description": fqn.sanitize_display_name(bucket.description),
                "database": fqn.database_fqn(self.service_name, self.project),
                "sourceUrl": self._bucket_url(bucket.id),
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

    def table_body(self, bucket: SourceBucket, table: SourceTable) -> BuiltTable:
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

        body: dict = {
            "name": fqn.sanitize_name(table.name),
            "displayName": fqn.sanitize_display_name(table.display_name or table.name),
            "description": fqn.sanitize_display_name(table.description),
            "tableType": table_type,
            "columns": columns,
            "databaseSchema": fqn.schema_fqn(self.service_name, self.project, bucket_path),
            "sourceUrl": self._table_url(bucket.id, table.id),
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
        source_table_name = parts[2]
        project_info = source.get("project") or {}
        source_project = project_info.get("name") or self.project
        return fqn.table_fqn(self.service_name, source_project, source_bucket_path, source_table_name)
