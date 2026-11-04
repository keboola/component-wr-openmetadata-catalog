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
    names = {"keboola.ex-db-mysql-1": ["Extract"], "keboola.snowflake-transformation-2": ["Transform"]}
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
        "limit": 20,
        "sortBy": "id",
        "sortOrder": "desc",
        "offset": 0,
    }


def test_child_jobs_are_found_by_the_parent_run_id():
    session = mock.Mock()
    session.get.return_value = _response([{"id": "2", "parentRunId": "1.2"}])
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert reader.child_jobs("1.2") == [{"id": "2", "parentRunId": "1.2"}]
    assert session.get.call_args.kwargs["params"]["parentRunId"] == "1.2"


def test_a_tasks_own_row_jobs_do_not_stand_for_the_task():
    """The search returns every descendant: an extractor task that failed must not
    show the success of its newest row job (seen live on a flow's GitHub task)."""
    session = mock.Mock()
    session.get.return_value = _response(
        [
            {"id": "63", "parentRunId": "37.49", "component": "keboola.ex-github-v2", "config": "1", "status": "error"},
            {
                "id": "18",
                "parentRunId": "37.49.63",
                "component": "keboola.ex-github-v2",
                "config": "1",
                "status": "success",
            },
        ]
    )
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    children = reader.child_jobs("37.49")
    assert [job["id"] for job in children] == ["63"]
    record = status_record({"status": "error"}, children, {"keboola.ex-github-v2-1": ["Extract"]})
    assert record.task_status == [{"name": "Extract", "executionStatus": "Failed"}]


def test_rate_limited_queue_call_is_retried():
    session = mock.Mock()
    session.get.side_effect = [_response({}, status_code=429), _response([])]
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session, backoff_base=0)
    assert reader.latest_job("keboola.ex-db-mysql", "1") is None
    assert session.get.call_count == 2


def test_dev_branch_jobs_are_ignored():
    """A dev branch keeps the production config id: only a production job sets the status."""
    session = mock.Mock()
    session.get.return_value = _response(
        [{"id": "3", "status": "error", "branchId": "777"}, {"id": "2", "status": "success", "branchId": "10893"}]
    )
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert reader.latest_job("keboola.ex-db-mysql", "1", branch_id="10893") == {
        "id": "2",
        "status": "success",
        "branchId": "10893",
    }


def test_a_retried_task_reports_its_newest_job():
    children = [
        {"id": "10", "component": "keboola.ex-db-mysql", "config": "1", "status": "error"},
        {"id": "12", "component": "keboola.ex-db-mysql", "config": "1", "status": "success"},
    ]
    record = status_record({"status": "success"}, children, {"keboola.ex-db-mysql-1": ["Extract"]})
    assert record.task_status == [{"name": "Extract", "executionStatus": "Successful"}]


def test_production_job_behind_a_page_of_dev_branch_jobs_is_found():
    session = mock.Mock()
    dev_page = [{"id": str(100 - i), "status": "error", "branchId": "777"} for i in range(20)]
    session.get.side_effect = [_response(dev_page), _response([{"id": "5", "status": "success", "branchId": "1"}])]
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert reader.latest_job("keboola.ex-db-mysql", "1", branch_id="1") == {
        "id": "5",
        "status": "success",
        "branchId": "1",
    }
    assert session.get.call_args.kwargs["params"]["offset"] == 20


def test_two_tasks_running_one_config_each_get_their_job():
    """A child job carries no task id: jobs of one config pair with its tasks in phase order."""
    children = [
        {"id": "10", "component": "keboola.ex-db-mysql", "config": "1", "status": "success"},
        {"id": "12", "component": "keboola.ex-db-mysql", "config": "1", "status": "error"},
    ]
    record = status_record({"status": "error"}, children, {"keboola.ex-db-mysql-1": ["Early", "Late"]})
    assert record.task_status == [
        {"name": "Early", "executionStatus": "Successful"},
        {"name": "Late", "executionStatus": "Failed"},
    ]


def _row_run():
    container = {
        "id": "63",
        "runId": "37.49.63",
        "parentRunId": "37.49",
        "type": "container",
        "component": "keboola.ex-github-v2",
        "config": "1",
        "status": "error",
        "configRowIds": [],
    }
    row_ok = {
        "id": "18",
        "runId": "37.49.63.18",
        "parentRunId": "37.49.63",
        "type": "standard",
        "component": "keboola.ex-github-v2",
        "config": "1",
        "status": "success",
        "configRowIds": ["r2"],
    }
    return container, row_ok


def test_a_row_based_config_reports_its_container_job_not_its_newest_row():
    """Seen live: the newest job of a row-based extractor was a successful row while
    the run (its container job) had failed."""
    container, row_ok = _row_run()
    row_ok["id"] = "64"  # rows start after the container
    session = mock.Mock()
    session.get.return_value = _response([row_ok, container])
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert (reader.latest_job("keboola.ex-github-v2", "1") or {}).get("id") == "63"


def test_a_row_job_whose_container_is_on_another_page_is_still_skipped():
    container, row_ok = _row_run()
    session = mock.Mock()
    session.get.side_effect = [_response([row_ok]), _response(container), _response([])]
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert reader.latest_job("keboola.ex-github-v2", "1") is None
    assert session.get.call_args_list[1].args[0].endswith("/jobs/63")


def test_a_single_row_run_by_hand_is_the_latest_job():
    """Running one row by hand makes a job with a row id but no container."""
    _, row_ok = _row_run()
    row_ok.pop("parentRunId")
    session = mock.Mock()
    session.get.return_value = _response([row_ok])
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    assert (reader.latest_job("keboola.ex-github-v2", "1") or {}).get("id") == "18"
