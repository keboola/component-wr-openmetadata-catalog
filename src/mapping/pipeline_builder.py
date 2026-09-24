"""Pipeline entity builder (E13/E14/E15, spec 2.1 / 4, T11).

Component configs -> Pipeline + Tasks (``taskSQL``, ``downstreamTasks``); flows
/ orchestrations -> Pipeline + Tasks with phase ordering; Job Queue run history
-> a ``PUT /pipelines/{fqn}/status`` body.

Structured metadata (component/config id, a public deep link, last change,
owner e-mail) is additionally written as OpenMetadata **custom properties** in
the ``extension``, mirroring ``mapping.dashboard_builder``'s treatment of
data-app Dashboards — a component config or flow always carries a real
creator e-mail (``currentVersion.creatorToken.description``), so this is the
main native-``owners`` win of the generalised treatment. Flows share the same
extension/owner logic as component configs (both are Keboola *configurations*
with the same creator-token shape).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from mapping import enrichment, fqn
from mapping.dashboard_builder import DATA_APP_COMPONENT_ID

_SERVICE_TYPE = "CustomPipeline"

# Custom properties defined on the OpenMetadata Pipeline type: (name, field
# type, description). om_types used across this component are only ever
# "string", "email", "hyperlink-cp".
CUSTOM_PROPERTIES = [
    ("kbcComponentId", "string", "Keboola component id"),
    ("kbcConfigId", "string", "Keboola configuration (or flow) id"),
    ("kbcConfigUrl", "hyperlink-cp", "Keboola configuration URL"),
    ("kbcLastChange", "string", "Last configuration change"),
    ("kbcOwner", "email", "Pipeline owner (last configuration editor)"),
    ("kbcSyncedAt", "string", "Catalog snapshot time (UTC) — the fields above are as of this time"),
    ("kbcType", "string", "Keboola object kind"),
]

_FLOW_COMPONENT_IDS = frozenset({"keboola.orchestrator", "keboola.flow"})
_SQL_COMPONENT_IDS = frozenset(
    {
        "keboola.snowflake-transformation",
        "keboola.snowflake-transformation-v2",
        "keboola.google-bigquery-transformation",
        "keboola.google-bigquery-transformation-v2",
        "keboola.redshift-transformation",
        "keboola.synapse-transformation",
        "keboola.exasol-transformation",
    }
)
_PYTHON_COMPONENT_IDS = frozenset(
    {"keboola.python-transformation-v2", "keboola.python-transformation", "keboola.r-transformation-v2"}
)

# Storage API id-pattern fallback for the extractor/writer families, used when a
# component's own `type` field is absent or unrecognised (see `component_kind`).
_EXTRACTOR_ID_MARKER = ".ex-"
_WRITER_ID_MARKER = ".wr-"

# `type` values the Storage API components endpoint is known to return, mapped
# 1:1 onto this catalog's object-family kinds.
_KIND_BY_COMPONENT_TYPE = frozenset({"transformation", "extractor", "writer", "application"})


def is_flow_component(component_id: str) -> bool:
    """True when ``component_id`` is a Keboola flow / orchestration component.

    A pure function of the id (no builder state), so the sync actions can reuse
    it without a project-bound ``PipelineBuilder``.
    """
    return component_id in _FLOW_COMPONENT_IDS


def component_kind(component_id: str, component_type: str | None = None) -> str:
    """Classify one Keboola component into an object-family kind.

    Returns one of ``"transformation" | "extractor" | "writer" | "application" |
    "orchestration" | "data_app" | "other"``. Verified against a reference
    connector hitting the same ``GET /v2/storage/branch/{branch}/components``
    endpoint this component's ``list_component_configs()`` calls: the Storage
    API DOES return a ``type`` field per component (extractor/writer/
    application/transformation/...), so it is preferred here. The id-pattern
    (``.ex-``/``.wr-``) and transformation id-set fallback stays in place for
    defensiveness -- a ``type`` that is missing, unrecognised, or belongs to a
    component family this catalog does not special-case (e.g. ``processor``)
    still resolves to a sensible kind instead of raising.
    """
    if is_flow_component(component_id):
        return "orchestration"
    if component_id == DATA_APP_COMPONENT_ID:
        return "data_app"
    normalized_type = (component_type or "").strip().lower()
    if normalized_type in _KIND_BY_COMPONENT_TYPE:
        return normalized_type
    if component_id in _SQL_COMPONENT_IDS or component_id in _PYTHON_COMPONENT_IDS:
        return "transformation"
    if _EXTRACTOR_ID_MARKER in component_id:
        return "extractor"
    if _WRITER_ID_MARKER in component_id:
        return "writer"
    return "other"


@dataclass
class BuiltPipeline:
    body: dict
    fqn: str
    component_id: str
    config_id: str


def _drop_none(body: dict) -> dict:
    return {k: v for k, v in body.items() if v is not None}


def _task_type(component_id: str) -> str:
    if component_id in _SQL_COMPONENT_IDS:
        return "QUERY"
    if component_id in _PYTHON_COMPONENT_IDS:
        return "PYTHON"
    return "TASK"


class PipelineBuilder:
    """Builds Pipeline bodies for one project."""

    def __init__(
        self, service_name: str, project: str, project_id: str | None, ui_base: str, stack_id: str | None = None
    ) -> None:
        self.service_name = service_name
        self.project = project
        self.project_id = project_id or "unknown"
        self.ui_base = ui_base.rstrip("/")
        self.stack_id = stack_id

    def pipeline_service_body(self) -> dict:
        return {
            "name": fqn.sanitize_name(self.service_name),
            "serviceType": _SERVICE_TYPE,
            "description": "Keboola pipelines catalogued by keboola.wr-openmetadata-catalog.",
        }

    def is_flow(self, component_id: str) -> bool:
        return is_flow_component(component_id)

    def _component_url(self, component_id: str, config_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/components/{component_id}/{config_id}"

    def _flow_url(self, flow_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/flows/{flow_id}"

    def _public_base(self) -> str:
        """Public Keboola connection base URL (``KBC_STACKID``, falls back to ``ui_base``)."""
        return enrichment.connection_base(self.stack_id, self.ui_base)

    def _kbc_component_url(self, component_id: str, config_id: str) -> str:
        return f"{self._public_base()}/admin/projects/{self.project_id}/components/{component_id}/{config_id}"

    def _kbc_flow_url(self, flow_id: str) -> str:
        return f"{self._public_base()}/admin/projects/{self.project_id}/flows/{flow_id}"

    @staticmethod
    def _extension(
        component_id: str,
        config_id: str,
        config: dict,
        config_url: str,
        available: set[str] | None,
        synced_at: str | None,
        kind: str | None = None,
    ) -> dict | None:
        values = {
            "kbcComponentId": component_id,
            "kbcConfigId": config_id,
            "kbcConfigUrl": enrichment.hyperlink(config_url, "Open configuration"),
            "kbcLastChange": enrichment.config_last_change(config),
            "kbcOwner": enrichment.creator_token_email(config),
            "kbcSyncedAt": synced_at,
            "kbcType": kind,
        }
        extension = {k: v for k, v in values.items() if v is not None and (available is None or k in available)}
        return extension or None

    @staticmethod
    def _owners(config: dict, owner_resolver: Callable[[str], str | None] | None) -> list[dict] | None:
        return enrichment.native_owners(enrichment.creator_token_email(config), owner_resolver)

    # --------------------------------------------------------------- E13/E14

    def build_pipeline(
        self,
        component_id: str,
        config: dict,
        *,
        kind: str | None = None,
        available_properties: set[str] | None = None,
        synced_at: str | None = None,
        owner_resolver: Callable[[str], str | None] | None = None,
    ) -> BuiltPipeline:
        """Dispatch to the flow or component-config builder.

        ``kind`` (one of ``component_kind``'s return values) populates the
        ``kbcType`` custom property when given; it is optional and purely
        additive, so a caller that predates the object-family split keeps
        working unchanged (no ``kbcType`` value is written).
        """
        if self.is_flow(component_id):
            return self._flow_pipeline(component_id, config, kind, available_properties, synced_at, owner_resolver)
        return self._config_pipeline(component_id, config, kind, available_properties, synced_at, owner_resolver)

    def _config_pipeline(
        self,
        component_id: str,
        config: dict,
        kind: str | None = None,
        available: set[str] | None = None,
        synced_at: str | None = None,
        owner_resolver: Callable[[str], str | None] | None = None,
    ) -> BuiltPipeline:
        config_id = str(config.get("id"))
        configuration = config.get("configuration") or {}
        tasks = self._blocks_to_tasks(component_id, configuration)
        if not tasks:
            tasks = self._rows_to_tasks(component_id, config.get("rows") or [])
        config_url = self._kbc_component_url(component_id, config_id)
        body = _drop_none(
            {
                "name": fqn.pipeline_name(self.project, config_id),
                "displayName": fqn.sanitize_display_name(config.get("name")),
                "description": fqn.sanitize_display_name(config.get("description")),
                "service": fqn.database_service_fqn(self.service_name),
                "sourceUrl": self._component_url(component_id, config_id),
                "tasks": tasks or None,
                "extension": self._extension(component_id, config_id, config, config_url, available, synced_at, kind),
                "owners": self._owners(config, owner_resolver),
            }
        )
        return BuiltPipeline(
            body=body,
            fqn=fqn.pipeline_fqn(self.service_name, self.project, config_id),
            component_id=component_id,
            config_id=config_id,
        )

    def _blocks_to_tasks(self, component_id: str, configuration: dict) -> list[dict]:
        blocks = ((configuration.get("parameters") or {}).get("blocks")) or []
        task_type = _task_type(component_id)
        tasks: list[dict] = []
        for i, block in enumerate(blocks):
            next_block = blocks[i + 1] if i + 1 < len(blocks) else None
            downstream = [fqn.sanitize_name(next_block.get("name"))] if next_block else []
            scripts = [s for code in block.get("codes") or [] for s in code.get("script") or []]
            task_sql = ";\n".join(scripts) if scripts else None
            tasks.append(
                _drop_none(
                    {
                        "name": fqn.sanitize_name(block.get("name")),
                        "displayName": fqn.sanitize_display_name(block.get("name")),
                        "taskType": task_type,
                        "taskSQL": task_sql,
                        "downstreamTasks": downstream,
                    }
                )
            )
        return tasks

    def _rows_to_tasks(self, component_id: str, rows: list) -> list[dict]:
        task_type = _task_type(component_id)
        tasks: list[dict] = []
        for row in rows:
            if row.get("isDisabled"):
                continue
            params = (row.get("configuration") or {}).get("parameters") or {}
            query = params.get("query")
            task_sql = (
                query if isinstance(query, str) else (query or {}).get("query") if isinstance(query, dict) else None
            )
            tasks.append(
                _drop_none(
                    {
                        "name": fqn.sanitize_name(row.get("name") or row.get("id")),
                        "displayName": fqn.sanitize_display_name(row.get("name")),
                        "taskType": task_type,
                        "taskSQL": task_sql,
                        "downstreamTasks": [],
                    }
                )
            )
        return tasks

    def _flow_pipeline(
        self,
        component_id: str,
        config: dict,
        kind: str | None = None,
        available: set[str] | None = None,
        synced_at: str | None = None,
        owner_resolver: Callable[[str], str | None] | None = None,
    ) -> BuiltPipeline:
        flow_id = str(config.get("id"))
        configuration = config.get("configuration") or {}
        tasks = self._flow_to_tasks(configuration)
        config_url = self._kbc_flow_url(flow_id)
        body = _drop_none(
            {
                "name": fqn.pipeline_name(self.project, flow_id),
                "displayName": fqn.sanitize_display_name(config.get("name")),
                "description": fqn.sanitize_display_name(config.get("description")),
                "service": fqn.database_service_fqn(self.service_name),
                "sourceUrl": self._flow_url(flow_id),
                "tasks": tasks or None,
                "extension": self._extension(component_id, flow_id, config, config_url, available, synced_at, kind),
                "owners": self._owners(config, owner_resolver),
            }
        )
        return BuiltPipeline(
            body=body,
            fqn=fqn.pipeline_fqn(self.service_name, self.project, flow_id),
            component_id=component_id,
            config_id=flow_id,
        )

    @staticmethod
    def _flow_to_tasks(configuration: dict) -> list[dict]:
        phases = configuration.get("phases") or []
        raw_tasks = configuration.get("tasks") or []
        phase_order = [p.get("id") for p in phases]
        tasks_by_phase: dict[object, list[dict]] = {}
        for task in raw_tasks:
            if task.get("enabled") is False:  # disabled tasks are excluded entirely (incl. downstream)
                continue
            tasks_by_phase.setdefault(task.get("phase"), []).append(task)

        tasks: list[dict] = []
        for idx, phase_id in enumerate(phase_order):
            next_phase = phase_order[idx + 1] if idx + 1 < len(phase_order) else None
            downstream = [fqn.sanitize_name(t.get("name") or t.get("id")) for t in tasks_by_phase.get(next_phase, [])]
            for task in tasks_by_phase.get(phase_id, []):
                tasks.append(
                    _drop_none(
                        {
                            "name": fqn.sanitize_name(task.get("name") or task.get("id")),
                            "displayName": fqn.sanitize_display_name(task.get("name")),
                            "taskType": "TASK",
                            "downstreamTasks": downstream,
                        }
                    )
                )
        return tasks
