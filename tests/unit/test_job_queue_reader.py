from unittest import mock

from client.job_queue_reader import JobQueueReader, execution_status, status_record


def _response(body, status_code=200):
    response = mock.Mock(status_code=status_code)
    response.json.return_value = body
    return response


def test_failed_job_is_failed_even_though_its_lineage_events_say_complete():
    """Keboola emits OpenLineage COMPLETE for a failed job too: the job's own status decides."""
    job = {"status": "error", "startTime": "2026-08-24T10:00:00+00:00", "endTime": "2026-08-24T10:02:00+00:00"}
    body = status_record(job, [], {}).to_status_body()
    assert body == {"timestamp": 1787565720000, "executionStatus": "Failed", "taskStatus": []}


def test_job_statuses_map_to_om_execution_statuses():
    assert [execution_status(s) for s in ("success", "warning", "error", "terminated", "cancelled")] == [
        "Successful",
        "Successful",
        "Failed",
        "Failed",
        "Failed",
    ]
    assert {execution_status(s) for s in ("created", "waiting", "processing", "terminating", None)} == {"Pending"}


def test_flow_child_jobs_become_task_statuses_under_the_pipeline_task_names():
    job = {"status": "success", "endTime": "2026-08-24T10:02:00+00:00"}
    children = [
        {"component": "keboola.ex-db-mysql", "config": "1", "status": "success"},
        {"component": "keboola.snowflake-transformation", "config": "2", "status": "error"},
        {"component": "keboola.other", "config": "3", "status": "success"},  # not a task of this flow
    ]
    names = {"keboola.ex-db-mysql-1": "Extract", "keboola.snowflake-transformation-2": "Transform"}
    record = status_record(job, children, names)
    assert record.task_status == [
        {"name": "Extract", "executionStatus": "Successful"},
        {"name": "Transform", "executionStatus": "Failed"},
    ]


def test_latest_job_asks_for_the_newest_job_of_one_config():
    session = mock.Mock()
    session.get.side_effect = [_response([{"id": "60770133", "status": "success"}]), _response([])]
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert reader.latest_job("keboola.ex-db-mysql", "123") == {"id": "60770133", "status": "success"}
    assert reader.latest_job("keboola.ex-db-mysql", "456") is None
    params = session.get.call_args_list[0].kwargs["params"]
    assert params == {
        "componentId": "keboola.ex-db-mysql",
        "configId": "123",
        "limit": 1,
        "sortBy": "id",
        "sortOrder": "desc",
    }


def test_child_jobs_are_found_by_the_parent_run_id():
    session = mock.Mock()
    session.get.return_value = _response([{"id": "2"}])
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert reader.child_jobs("1.2") == [{"id": "2"}]
    assert session.get.call_args.kwargs["params"]["parentRunId"] == "1.2"


def test_rate_limited_queue_call_is_retried():
    session = mock.Mock()
    session.get.side_effect = [_response({}, status_code=429), _response([])]
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session, backoff_base=0)
    assert reader.latest_job("keboola.ex-db-mysql", "1") is None
    assert session.get.call_count == 2
