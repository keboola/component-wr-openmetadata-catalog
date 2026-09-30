"""Job Queue run-history reader (E15, spec 2.1 / research 9.2).

``GET /jobs/{jobId}/open-api-lineage`` returns OpenLineage START/COMPLETE
events (including child jobs of an orchestration). These map to an OM Pipeline
status body (``PUT /pipelines/{fqn}/status``): success/fail plus start/end
timestamps.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

import requests
from keboola.component.exceptions import UserException

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = frozenset({500, 502, 503, 504})
_START = "START"
_TERMINAL = frozenset({"COMPLETE", "FAIL", "ABORT"})
_SUCCESS_STATUS = "Successful"
_FAILED_STATUS = "Failed"
_PENDING_STATUS = "Pending"


@dataclass
class PipelineStatusRecord:
    execution_status: str
    timestamp_ms: int
    task_status: list[dict] = field(default_factory=list)

    def to_status_body(self) -> dict:
        body: dict = {"timestamp": self.timestamp_ms, "executionStatus": self.execution_status}
        if self.task_status:
            body["taskStatus"] = self.task_status
        return body


def _epoch_ms(iso: str | None) -> int | None:
    if not iso:
        return None
    try:
        return int(datetime.fromisoformat(iso).timestamp() * 1000)
    except ValueError, AttributeError:
        return None


def _status_from_event_type(event_type: str) -> str:
    if event_type == "COMPLETE":
        return _SUCCESS_STATUS
    if event_type in ("FAIL", "ABORT"):
        return _FAILED_STATUS
    return _PENDING_STATUS


class JobQueueReader:
    """Reads a job's OpenLineage run history from the Job Queue API."""

    def __init__(
        self,
        base_url: str,
        storage_token: str,
        *,
        timeout: int = 60,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.session = session or requests.Session()
        self.session.headers.update({"X-StorageApi-Token": storage_token, "Accept": "application/json"})

    def _get(self, path: str) -> object:
        url = f"{self.base_url}{path}"
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    time.sleep(self.backoff_base * (2**attempt))
                    continue
                raise UserException(f"Job Queue request failed: GET {path}: {exc}") from exc
            if response.status_code in _RETRYABLE_STATUS and attempt < self.max_retries:
                time.sleep(self.backoff_base * (2**attempt))
                continue
            if response.status_code >= 400:
                raise UserException(f"Job Queue GET {path} failed: {response.status_code}")
            return response.json()
        raise UserException(f"Job Queue request exhausted retries: GET {path}")

    def get_lineage_events(self, job_id: str) -> list[dict]:
        raw = self._get(f"/jobs/{job_id}/open-api-lineage")
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            return raw.get("events") or raw.get("data") or []
        return []

    @staticmethod
    def summarize_run(events: list[dict]) -> PipelineStatusRecord | None:
        """Fold OpenLineage events (parent + children) into one status record."""
        if not events:
            return None
        start_ms: int | None = None
        end_ms: int | None = None
        overall = _SUCCESS_STATUS
        saw_terminal = False
        task_status: list[dict] = []

        for event in events:
            event_type = (event.get("eventType") or "").upper()
            ts = _epoch_ms(event.get("eventTime"))
            job_name = (event.get("job") or {}).get("name")

            if event_type == _START and ts is not None:
                start_ms = ts if start_ms is None else min(start_ms, ts)
            if event_type in _TERMINAL:
                saw_terminal = True
                if ts is not None:
                    end_ms = ts if end_ms is None else max(end_ms, ts)
                status = _status_from_event_type(event_type)
                if status == _FAILED_STATUS:
                    overall = _FAILED_STATUS
                if job_name:
                    task_status.append({"name": job_name, "executionStatus": status})

        if not saw_terminal:
            overall = _PENDING_STATUS
        timestamp = end_ms or start_ms or int(time.time() * 1000)
        return PipelineStatusRecord(execution_status=overall, timestamp_ms=timestamp, task_status=task_status)
