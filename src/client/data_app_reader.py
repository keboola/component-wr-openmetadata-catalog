"""Reader for Keboola Data Science data-app deployment state.

The data-app deployment state (running / stopped / created / ...) is not part of
the Storage configuration; it lives in the Data Science service. It is fetched
here best-effort so a data-app Dashboard can carry the same Status shown in the
Keboola UI. Any failure returns an empty map and the dashboards are simply
written without a status.
"""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)


def fetch_app_states(data_science_url: str, storage_token: str, *, timeout: int = 30) -> dict[str, str]:
    """Return ``{config_id: state}`` for the project's data apps (empty on any failure)."""
    try:
        response = requests.get(
            f"{data_science_url.rstrip('/')}/apps",
            headers={"X-StorageApi-Token": storage_token, "Accept": "application/json"},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("Could not fetch data-app states (%s); dashboards written without status", exc)
        return {}

    apps = payload if isinstance(payload, list) else (payload.get("apps") or payload.get("data") or [])
    states: dict[str, str] = {}
    for app in apps:
        config_id = app.get("configId") or app.get("config_id")
        state = app.get("state") or app.get("desiredState")
        if config_id and state:
            states[str(config_id)] = str(state)
    return states
