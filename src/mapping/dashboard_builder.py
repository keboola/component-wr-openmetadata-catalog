"""Dashboard entity builder — Keboola data apps -> OpenMetadata Dashboard.

Data apps (component ``keboola.data-apps``) carry no Storage input/output
mapping, so they never qualify as Pipelines (they read/write at runtime through a
workspace, not through the config's storage mapping). They are catalogued instead
as OpenMetadata Dashboard entities under a single ``CustomDashboard`` service.

Structured metadata (App ID, owner, last change, the deployed app URL, and the
Keboola configuration URL) is written as OpenMetadata **custom properties** in the
entity ``extension``, so it renders as typed fields rather than free text. The app
and configuration URLs are built from ``KBC_STACKID`` (the public stack domain),
so they are correct even when the job runs on-platform with an internal ``KBC_URL``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from mapping import enrichment, fqn

_SERVICE_TYPE = "CustomDashboard"
DATA_APP_COMPONENT_ID = "keboola.data-apps"

# Custom properties defined on the OpenMetadata Dashboard type: (name, field type, description).
CUSTOM_PROPERTIES = [
    ("kbcAppId", "string", "Keboola data app id"),
    ("kbcStatus", "string", "Data app deployment status"),
    ("kbcOwner", "email", "Data app owner (last configuration editor)"),
    ("kbcLastChange", "string", "Last configuration change"),
    ("kbcSyncedAt", "string", "Catalog snapshot time (UTC) — status is as of this time"),
    ("kbcAppUrl", "hyperlink-cp", "Deployed data app URL"),
    ("kbcConfigUrl", "hyperlink-cp", "Keboola configuration URL"),
]

# Data Science deployment state -> the label shown in the Keboola UI Status column.
_STATUS_LABELS = {"running": "ACTIVE", "starting": "STARTING", "stopped": "STOPPED", "created": "NOT DEPLOYED"}


@dataclass
class BuiltDashboard:
    body: dict
    fqn: str
    config_id: str


def _drop_none(body: dict) -> dict:
    return {k: v for k, v in body.items() if v is not None}


class DashboardBuilder:
    """Builds Dashboard bodies (one per data-app config) for one project."""

    def __init__(
        self, service_name: str, project: str, project_id: str | None, ui_base: str, stack_id: str | None = None
    ) -> None:
        self.service_name = service_name
        self.project = project
        self.project_id = project_id or "unknown"
        self.ui_base = ui_base.rstrip("/")
        self.stack_id = stack_id

    def dashboard_service_body(self) -> dict:
        return {
            "name": fqn.sanitize_name(self.service_name),
            "serviceType": _SERVICE_TYPE,
            "description": "Keboola data apps catalogued by keboola.wr-openmetadata-catalog.",
        }

    @staticmethod
    def is_data_app(component_id: str) -> bool:
        return component_id == DATA_APP_COMPONENT_ID

    def _connection_base(self) -> str:
        """Public Keboola connection base URL from ``KBC_STACKID`` (falls back to ui_base)."""
        return enrichment.connection_base(self.stack_id, self.ui_base)

    def _config_url(self, config_id: str) -> str:
        return f"{self._connection_base()}/admin/projects/{self.project_id}/data-apps/{config_id}"

    def _app_url(self, slug: str | None, app_id: str | None) -> str | None:
        """Deployed data-app URL ``https://<slug>-<appId>.hub.<region>`` from ``KBC_STACKID``."""
        if not (slug and app_id and self.stack_id):
            return None
        region = self.stack_id.removeprefix("connection.")
        return f"https://{slug}-{app_id}.hub.{region}"

    # Owner e-mail extracted from the creator-token description — shared with
    # every other builder that catalogs a Keboola configuration (pipelines,
    # flows); see ``mapping.enrichment.creator_token_email``.
    _owner = staticmethod(enrichment.creator_token_email)

    # Last-change timestamp — shared with ``mapping.pipeline_builder``.
    _last_change = staticmethod(enrichment.config_last_change)

    _hyperlink = staticmethod(enrichment.hyperlink)

    @classmethod
    def _owners(cls, config: dict, owner_resolver: Callable[[str], str | None] | None) -> list[dict] | None:
        """Native OM ``owners`` for the app's owner e-mail, when a matching OM user exists.

        The owner e-mail (the last configuration editor) is resolved to an OM user id
        by ``owner_resolver``. When no resolver is given, or the e-mail has no OM user,
        no native owner is set — the ``kbcOwner`` custom property still records the
        e-mail. Owner assignment is therefore additive and best-effort.
        """
        return enrichment.native_owners(cls._owner(config), owner_resolver)

    @staticmethod
    def _status_label(state: str | None) -> str | None:
        """Map a Data Science deployment state to the Keboola UI Status label."""
        return _STATUS_LABELS.get(state, state.upper()) if state else None

    def _extension(
        self, config: dict, available: set[str] | None, app_states: dict[str, str] | None, synced_at: str | None
    ) -> dict | None:
        params = (config.get("configuration") or {}).get("parameters") or {}
        app_id = params.get("id")
        config_id = str(config.get("id"))
        slug = (params.get("dataApp") or {}).get("slug")
        values = {
            "kbcAppId": app_id,
            "kbcStatus": self._status_label((app_states or {}).get(config_id)),
            "kbcOwner": self._owner(config),
            "kbcLastChange": self._last_change(config),
            "kbcSyncedAt": synced_at,
            "kbcAppUrl": self._hyperlink(self._app_url(slug, app_id), "Open app"),
            "kbcConfigUrl": self._hyperlink(self._config_url(config_id), "Open configuration"),
        }
        extension = {k: v for k, v in values.items() if v is not None and (available is None or k in available)}
        return extension or None

    def build_dashboard(
        self,
        config: dict,
        available_properties: set[str] | None = None,
        app_states: dict[str, str] | None = None,
        synced_at: str | None = None,
        owner_resolver: Callable[[str], str | None] | None = None,
    ) -> BuiltDashboard:
        config_id = str(config.get("id"))
        params = (config.get("configuration") or {}).get("parameters") or {}
        app_url = self._app_url((params.get("dataApp") or {}).get("slug"), params.get("id"))
        body = _drop_none(
            {
                "name": fqn.dashboard_name(self.project, config_id),
                "displayName": fqn.sanitize_display_name(config.get("name")),
                # Always emit description (empty when the app has none) so it overwrites
                # any earlier value rather than leaving a stale one in place.
                "description": fqn.sanitize_display_name(config.get("description")) or "",
                "service": fqn.dashboard_service_fqn(self.service_name),
                "sourceUrl": app_url or self._config_url(config_id),
                "owners": self._owners(config, owner_resolver),
                "extension": self._extension(config, available_properties, app_states, synced_at),
            }
        )
        return BuiltDashboard(
            body=body,
            fqn=fqn.dashboard_fqn(self.service_name, self.project, config_id),
            config_id=config_id,
        )
