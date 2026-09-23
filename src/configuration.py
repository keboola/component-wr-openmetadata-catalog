"""Pydantic configuration model for the OpenMetadata catalog writer.

The Keboola platform merges the root config and the row config into a single
``config.json`` before each row runs (see spec 2.2), so this single
``Configuration`` model validates the merged root+row parameter shape. It is
validated at construction; any validation error is surfaced as a
``UserException`` so the component exits 1 (a user-fixable config error).
"""

from __future__ import annotations

import logging
from enum import StrEnum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

logger = logging.getLogger(__name__)


class ProjectScope(StrEnum):
    """Which multi-project seam to use (spec 5.4)."""

    THIS_PROJECT = "this_project"
    ALL_PROJECTS = "all_projects"


class MergeMode(StrEnum):
    """How a divergence between OM and our last-written value is resolved."""

    THREE_WAY_MERGE = "three_way_merge"
    KEBOOLA_ALWAYS_WINS = "keboola_always_wins"


class FailureMode(StrEnum):
    """How per-entity write failures are handled (spec 6.3)."""

    COLLECT_AND_FAIL = "collect_and_fail"
    FAIL_FAST = "fail_fast"
    LOG_ONLY = "log_only"


class Ssh(BaseModel):
    """Optional SSH bastion tunnel around the OM host (spec 3.4)."""

    model_config = ConfigDict(populate_by_name=True)

    host: str
    user: str
    port: int = 22
    private_key: str = Field(alias="#private_key")


class Configuration(BaseModel):
    """Validated, merged root+row configuration.

    Instantiate with the raw merged ``parameters`` dict. A ``ValidationError``
    is re-raised as a ``UserException`` so the caller can exit 1.
    """

    model_config = ConfigDict(populate_by_name=True, use_enum_values=False, extra="ignore")

    # --- Connection (root) ---
    om_host: str
    bot_token: str = Field(alias="#bot_token")
    service_name: str | None = None

    # --- Scope / multi-project (row) ---
    scope: ProjectScope = ProjectScope.THIS_PROJECT
    manage_token: str | None = Field(default=None, alias="#manage_token")
    organization_id: str | None = None

    # --- Behaviour (row, Advanced) ---
    merge_mode: MergeMode = MergeMode.THREE_WAY_MERGE
    failure_mode: FailureMode = FailureMode.COLLECT_AND_FAIL
    # Per-object lineage toggles (spec: generalize the old single write_lineage /
    # write_column_lineage pair to one bool per lineage aspect). write_table_lineage
    # is the old write_lineage's table-level half; write_pipeline_lineage is the
    # pipeline-as-node half (see mapping.lineage_builder / component._lineage_pass).
    write_bucket_lineage: bool = True
    write_table_lineage: bool = True
    write_column_lineage: bool = True
    write_pipeline_lineage: bool = True
    write_dashboard_lineage: bool = True
    write_pipeline_status: bool = True
    full_refresh: bool = False

    # --- SSH (root) ---
    use_ssh_tunnel: bool = False
    ssh: Ssh | None = None

    # --- Row-level ---
    storage_token: str | None = Field(default=None, alias="#storage_token")
    project_name_override: str | None = None

    # --- Object families (spec: split the pipeline family into Transformations vs
    # Components; each family is an enable bool + a selector list, mirroring
    # ``buckets`` -- an empty selector means "all", scoped to THIS_PROJECT only). ---
    write_buckets: bool = True
    buckets: list[str] = Field(default_factory=list)
    write_transformations: bool = True
    transformations: list[str] = Field(default_factory=list)
    write_components: bool = True
    components: list[str] = Field(default_factory=list)
    write_flows: bool = True
    flows: list[str] = Field(default_factory=list)
    write_data_apps: bool = True
    data_apps: list[str] = Field(default_factory=list)
    projects: list[str] = Field(default_factory=list)

    def __init__(self, **data: object) -> None:
        try:
            super().__init__(**data)
        except ValidationError as e:
            messages = [f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration validation error: {'; '.join(messages)}") from e

    @model_validator(mode="before")
    @classmethod
    def _flatten_ui_groups(cls, data: object) -> object:
        """Lift the row schema's nested UI groups to the flat model shape.

        ``configRowSchema.json`` nests the object-family fields under ``objects``
        and the behaviour fields under ``advanced`` (with the five lineage bools
        one level deeper under ``advanced.lineage``), so the Keboola UI saves
        ``parameters`` with those nested containers. This model is flat, so the
        nested keys are lifted to the top level before field validation. Nested
        values win over any same-named top-level key (the nested group is the
        schema's authoritative home); a flat-only config (older configs, the
        functional fixtures) is passed through untouched.
        """
        if not isinstance(data, dict):
            return data
        flat = dict(data)
        objects = flat.pop("objects", None)
        if isinstance(objects, dict):
            flat.update(objects)
        advanced = flat.pop("advanced", None)
        if isinstance(advanced, dict):
            lineage = advanced.get("lineage")
            flat.update({k: v for k, v in advanced.items() if k != "lineage"})
            if isinstance(lineage, dict):
                flat.update(lineage)
        return flat

    @model_validator(mode="after")
    def _check_cross_fields(self) -> Configuration:
        """Cross-field rules that map to config-error exit codes (spec 6.3)."""
        if self.scope == ProjectScope.ALL_PROJECTS:
            missing: list[str] = []
            if not self.manage_token:
                missing.append("a Management API token (#manage_token)")
            if not self.organization_id:
                missing.append("an organization_id")
            if missing:
                raise UserException(
                    f"scope='all_projects' requires {' and '.join(missing)}. "
                    "Provide the missing value(s) or switch scope to 'this_project'."
                )
        if self.use_ssh_tunnel and self.ssh is None:
            raise UserException("use_ssh_tunnel is enabled but the 'ssh' configuration block is missing.")
        return self

    def resolve_service_name(self, stack_id: str | None) -> str:
        """Return the OM DatabaseService name (FQN root).

        Uses the explicit ``service_name`` override when set, otherwise derives
        a stable default from ``KBC_STACKID`` (spec 5.2). Never persisted.
        """
        if self.service_name:
            return self.service_name
        if stack_id:
            return f"keboola-{stack_id}".replace(".", "-")
        return "keboola"
