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

import re
from dataclasses import dataclass

from mapping import fqn

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")

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
        if self.stack_id:
            host = self.stack_id if self.stack_id.startswith("connection.") else f"connection.{self.stack_id}"
            return f"https://{host}"
        return self.ui_base

    def _config_url(self, config_id: str) -> str:
        return f"{self._connection_base()}/admin/projects/{self.project_id}/data-apps/{config_id}"

    def _app_url(self, slug: str | None, app_id: str | None) -> str | None:
        """Deployed data-app URL ``https://<slug>-<appId>.hub.<region>`` from ``KBC_STACKID``."""
        if not (slug and app_id and self.stack_id):
            return None
        region = self.stack_id.removeprefix("connection.")
        return f"https://{slug}-{app_id}.hub.{region}"

    @staticmethod
    def _owner(config: dict) -> str | None:
        """Owner e-mail extracted from the creator-token description.

        The token description is usually an e-mail, but can wrap one in text
        (``"kbagent-cli [martin@keboola.com]"``); the ``kbcOwner`` custom property
        is e-mail-typed, so extract the address and skip descriptions without one.
        """
        version = config.get("currentVersion") or {}
        token = version.get("creatorToken") or config.get("creatorToken") or {}
        match = _EMAIL.search(token.get("description") or "")
        return match.group(0) if match else None

    @staticmethod
    def _last_change(config: dict) -> str | None:
        version = config.get("currentVersion") or {}
        timestamp = version.get("created") or config.get("created")
        return timestamp.replace("T", " ")[:16] if timestamp else None

    @staticmethod
    def _hyperlink(url: str | None, display_text: str) -> dict | None:
        return {"url": url, "displayText": display_text} if url else None

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
                "extension": self._extension(config, available_properties, app_states, synced_at),
            }
        )
        return BuiltDashboard(
            body=body,
            fqn=fqn.dashboard_fqn(self.service_name, self.project, config_id),
            config_id=config_id,
        )
