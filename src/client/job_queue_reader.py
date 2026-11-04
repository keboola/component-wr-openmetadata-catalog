"""Job Queue run-history reader (E15, spec 2.1 / research 9.2).

A pipeline's status is its configuration's latest job (``GET /search/jobs``): the
job's own ``status`` and start/end time map to an OM Pipeline status body
(``PUT /pipelines/{fqn}/status``). A flow's child jobs (``parentRunId``) give the
status of each of its tasks. The job's OpenLineage events are not used: Keboola
emits ``COMPLETE`` for a failed job too.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

import requests
from keboola.component.exceptions import UserException

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_SUCCESS_STATUS = "Successful"
_FAILED_STATUS = "Failed"
_PENDING_STATUS = "Pending"
# Keboola job status -> OM execution status. Anything else (created, waiting,
# processing, terminating) has not finished yet.
_EXECUTION_STATUS = {
    "success": _SUCCESS_STATUS,
    "warning": _SUCCESS_STATUS,
    "error": _FAILED_STATUS,
    "terminated": _FAILED_STATUS,
    "cancelled": _FAILED_STATUS,
}
_MAX_CHILD_JOBS = 500
_LATEST_JOBS = 20
_MAX_JOB_PAGES = 10


@dataclass
class PipelineStatusRecord:
    execution_status: str
    timestamp_ms: int
    task_status: list[dict] = field(default_factory=list)

    def to_status_body(self) -> dict:
        # OM 1.13.4 fails with HTTP 500 on a status without ``taskStatus``: always send the list.
        return {
            "timestamp": self.timestamp_ms,
            "executionStatus": self.execution_status,
            "taskStatus": self.task_status,
        }


def _epoch_ms(iso: str | None) -> int | None:
    if not iso:
        return None
    try:
        return int(datetime.fromisoformat(iso).timestamp() * 1000)
    except ValueError, AttributeError:
        return None


def execution_status(job_status: str | None) -> str:
    return _EXECUTION_STATUS.get((job_status or "").lower(), _PENDING_STATUS)


def status_record(job: dict, children: list[dict], task_names: dict[str, list[str]]) -> PipelineStatusRecord:
    """The OM status of one job; ``task_names`` maps ``<component>-<config>`` of a child job to task names.

    A child job that maps to no task of the pipeline is left out: OM rejects a task
    status under a name the pipeline does not have (400). A child job carries no
    flow task id, so when several tasks run one configuration (in phase order) the
    newest of its jobs pair with them oldest first; a single task takes the newest
    job, as a retried task leaves several.
    """
    timestamp = (
        _epoch_ms(job.get("endTime"))
        or _epoch_ms(job.get("startTime"))
        or _epoch_ms(job.get("createdTime"))
        or int(time.time() * 1000)
    )
    runs: dict[str, list[dict]] = {}  # oldest first
    for child in sorted(children, key=lambda c: int(c.get("id") or 0)):
        runs.setdefault(f"{child.get('component')}-{child.get('config')}", []).append(child)
    tasks = []
    for key, names in task_names.items():
        key_runs = runs.get(key) or []
        latest = key_runs[-len(names) :] if key_runs else []
        tasks += [
            {"name": name, "executionStatus": execution_status(child.get("status"))}
            for name, child in zip(names, latest, strict=False)
        ]
    return PipelineStatusRecord(execution_status(job.get("status")), timestamp, tasks)


class JobQueueReader:
    """Reads configurations' latest jobs from the Job Queue API."""

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

    def _get(self, path: str, params: dict | None = None) -> object:
        url = f"{self.base_url}{path}"
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
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

    def latest_job(self, component_id: str, config_id: str, *, branch_id: str | None = None) -> dict | None:
        """The configuration's most recent job on ``branch_id`` (production), or ``None``.

        A dev branch keeps the production config id, so its jobs are skipped; the
        newest production job wins. Pages are read until one is found, up to
        ``_MAX_JOB_PAGES`` pages, so a run of dev-branch jobs does not hide it. A
        row-based config runs each row as a job of its own under one container
        job; the container's result is the run's, so the row jobs are skipped.
        """
        params = {"componentId": component_id, "configId": config_id, "limit": _LATEST_JOBS}
        for page in range(_MAX_JOB_PAGES):
            jobs = self._get(
                "/search/jobs", params | {"sortBy": "id", "sortOrder": "desc", "offset": page * _LATEST_JOBS}
            )
            jobs = jobs if isinstance(jobs, list) else []
            by_id = {str(job.get("id")): job for job in jobs}
            for job in jobs:
                on_branch = (
                    branch_id is None or job.get("branchId") is None or str(job.get("branchId")) == str(branch_id)
                )
                if on_branch and not self._is_row_job(job, by_id):
                    return job
            if len(jobs) < _LATEST_JOBS:
                break
        return None

    def _is_row_job(self, job: dict, by_id: dict[str, dict]) -> bool:
        """Whether a job runs one row of a config whose container job runs them all."""
        parent_run = str(job.get("parentRunId") or "")
        if not parent_run or not job.get("configRowIds"):
            return False
        parent_id = parent_run.rsplit(".", 1)[-1]
        parent = by_id.get(parent_id)
        if parent is None:
            fetched = self._get(f"/jobs/{parent_id}")
            parent = fetched if isinstance(fetched, dict) else {}
        return (
            parent.get("type") == "container"
            and parent.get("component") == job.get("component")
            and str(parent.get("config")) == str(job.get("config"))
        )

    def child_jobs(self, run_id: str) -> list[dict]:
        """The jobs a flow job started (its tasks).

        The search returns every descendant, including the jobs a task started in
        turn (a row-based component runs each row as a job of its own), and a
        row's success must not stand for its task's failure: only direct children
        are kept.
        """
        jobs = self._get("/search/jobs", {"parentRunId": run_id, "limit": _MAX_CHILD_JOBS})
        return [job for job in jobs if str(job.get("parentRunId")) == run_id] if isinstance(jobs, list) else []
