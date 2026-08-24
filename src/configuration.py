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

    ROWS = "rows"
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


class BranchFilter(StrEnum):
    """Which ``KBC.createdBy.branch.id`` scope to catalog."""

    PRODUCTION_ONLY = "production_only"
    ALL_BRANCHES = "all_branches"


class Stage(StrEnum):
    """Keboola bucket stages this component can catalog."""

    IN = "in"
    OUT = "out"


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

    # --- Scope / multi-project (root) ---
    project_scope: ProjectScope = ProjectScope.ROWS
    manage_token: str | None = Field(default=None, alias="#manage_token")
    organization_id: str | None = None

    # --- Behaviour (root, Advanced) ---
    merge_mode: MergeMode = MergeMode.THREE_WAY_MERGE
    failure_mode: FailureMode = FailureMode.COLLECT_AND_FAIL
    branch_filter: BranchFilter = BranchFilter.PRODUCTION_ONLY
    write_lineage: bool = True
    write_column_lineage: bool = True
    write_pipelines: bool = True
    write_pipeline_status: bool = True
    full_refresh: bool = False
    om_version_override: str | None = None
    debug: bool = False

    # --- SSH (root) ---
    use_ssh_tunnel: bool = False
    ssh: Ssh | None = None

    # --- Row-level ---
    storage_token: str | None = Field(default=None, alias="#storage_token")
    project_name_override: str | None = None
    stages: list[Stage] = Field(default_factory=lambda: [Stage.IN, Stage.OUT])
    bucket_allowlist: list[str] = Field(default_factory=list)
    bucket_denylist: list[str] = Field(default_factory=list)

    def __init__(self, **data: object) -> None:
        try:
            super().__init__(**data)
        except ValidationError as e:
            messages = [f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration validation error: {'; '.join(messages)}") from e

    @model_validator(mode="after")
    def _check_cross_fields(self) -> Configuration:
        """Cross-field rules that map to config-error exit codes (spec 6.3)."""
        if self.project_scope == ProjectScope.ALL_PROJECTS:
            missing: list[str] = []
            if not self.manage_token:
                missing.append("a Management API token (#manage_token)")
            if not self.organization_id:
                missing.append("an organization_id")
            if missing:
                raise UserException(
                    f"project_scope='all_projects' requires {' and '.join(missing)}. "
                    "Provide the missing value(s) or switch project_scope to 'rows'."
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
