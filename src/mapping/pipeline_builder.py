"""Pipeline entity builder (E13/E14/E15, spec 2.1 / 4, T11).

Component configs -> Pipeline + Tasks (``taskSQL``, ``downstreamTasks``); flows
/ orchestrations -> Pipeline + Tasks with phase ordering; Job Queue run history
-> a ``PUT /pipelines/{fqn}/status`` body.
"""

from __future__ import annotations

from dataclasses import dataclass

from mapping import fqn

_SERVICE_TYPE = "CustomPipeline"

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

    def __init__(self, service_name: str, project: str, project_id: str | None, ui_base: str) -> None:
        self.service_name = service_name
        self.project = project
        self.project_id = project_id or "unknown"
        self.ui_base = ui_base.rstrip("/")

    def pipeline_service_body(self) -> dict:
        return {
            "name": fqn.sanitize_name(self.service_name),
            "serviceType": _SERVICE_TYPE,
            "description": "Keboola pipelines catalogued by keboola.wr-openmetadata-catalog.",
        }

    def is_flow(self, component_id: str) -> bool:
        return component_id in _FLOW_COMPONENT_IDS

    def _component_url(self, component_id: str, config_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/components/{component_id}/{config_id}"

    def _flow_url(self, flow_id: str) -> str:
        return f"{self.ui_base}/admin/projects/{self.project_id}/flows/{flow_id}"

    # --------------------------------------------------------------- E13/E14

    def build_pipeline(self, component_id: str, config: dict) -> BuiltPipeline:
        """Dispatch to the flow or component-config builder."""
        if self.is_flow(component_id):
            return self._flow_pipeline(component_id, config)
        return self._config_pipeline(component_id, config)

    def _config_pipeline(self, component_id: str, config: dict) -> BuiltPipeline:
        config_id = str(config.get("id"))
        configuration = config.get("configuration") or {}
        tasks = self._blocks_to_tasks(component_id, configuration)
        if not tasks:
            tasks = self._rows_to_tasks(component_id, config.get("rows") or [])
        body = _drop_none(
            {
                "name": fqn.pipeline_name(self.project, config_id),
                "displayName": fqn.sanitize_display_name(config.get("name")),
                "description": fqn.sanitize_display_name(config.get("description")),
                "service": fqn.database_service_fqn(self.service_name),
                "sourceUrl": self._component_url(component_id, config_id),
                "tasks": tasks or None,
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

    def _flow_pipeline(self, component_id: str, config: dict) -> BuiltPipeline:
        flow_id = str(config.get("id"))
        configuration = config.get("configuration") or {}
        tasks = self._flow_to_tasks(configuration)
        body = _drop_none(
            {
                "name": fqn.pipeline_name(self.project, flow_id),
                "displayName": fqn.sanitize_display_name(config.get("name")),
                "description": fqn.sanitize_display_name(config.get("description")),
                "service": fqn.database_service_fqn(self.service_name),
                "sourceUrl": self._flow_url(flow_id),
                "tasks": tasks or None,
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
