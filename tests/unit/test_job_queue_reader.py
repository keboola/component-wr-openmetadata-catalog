from client.job_queue_reader import JobQueueReader


def test_summarize_run_success_with_children():
    events = [
        {"eventType": "START", "eventTime": "2026-08-24T10:00:00Z", "job": {"name": "flow"}},
        {"eventType": "START", "eventTime": "2026-08-24T10:00:05Z", "job": {"name": "child-1"}},
        {"eventType": "COMPLETE", "eventTime": "2026-08-24T10:01:00Z", "job": {"name": "child-1"}},
        {"eventType": "COMPLETE", "eventTime": "2026-08-24T10:02:00Z", "job": {"name": "flow"}},
    ]
    rec = JobQueueReader.summarize_run(events)
    assert rec is not None
    assert rec.execution_status == "Successful"
    body = rec.to_status_body()
    assert body["executionStatus"] == "Successful"
    # end timestamp is the latest terminal event (2026-08-24T10:02:00Z)
    assert body["timestamp"] == 1787565720000
    names = {t["name"] for t in body["taskStatus"]}
    assert {"child-1", "flow"} <= names


def test_summarize_run_failure_when_any_child_fails():
    events = [
        {"eventType": "START", "eventTime": "2026-08-24T10:00:00Z", "job": {"name": "flow"}},
        {"eventType": "FAIL", "eventTime": "2026-08-24T10:00:30Z", "job": {"name": "child-1"}},
        {"eventType": "COMPLETE", "eventTime": "2026-08-24T10:01:00Z", "job": {"name": "flow"}},
    ]
    rec = JobQueueReader.summarize_run(events)
    assert rec.execution_status == "Failed"


def test_summarize_run_empty_returns_none():
    assert JobQueueReader.summarize_run([]) is None


def test_get_lineage_events_unwraps_dict():
    from unittest import mock

    session = mock.Mock()

    class R:
        status_code = 200

        @staticmethod
        def json():
            return {"events": [{"eventType": "START", "eventTime": "2026-08-24T10:00:00Z"}]}

    session.get.return_value = R()
    reader = JobQueueReader("https://queue.keboola.com", "tok", session=session)
    events = reader.get_lineage_events("job-1")
    assert len(events) == 1
