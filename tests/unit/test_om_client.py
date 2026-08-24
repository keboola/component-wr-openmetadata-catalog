import json
from unittest import mock

import pytest
import requests

from client.om_client import (
    OMAuthError,
    OMClient,
    OMClientError,
    OMPreconditionFailed,
)


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.text = json.dumps(self._body)
        self.content = self.text.encode()

    def json(self):
        return self._body


def _client(session):
    return OMClient("https://om.example.com/", "tok", session=session, backoff_base=0)


def test_probe_version_parses_shape():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {"version": "1.13.4", "revision": "abc", "timestamp": 123})
    c = _client(session)
    data = c.probe_version()
    assert data["version"] == "1.13.4"
    assert c.server_version == "1.13.4"
    assert c.is_2_0_or_newer is False
    method, url = session.request.call_args[0]
    assert method == "GET"
    assert url == "https://om.example.com/api/v1/system/version"


def test_put_entity_service_path_vs_plain():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {"id": "1"})
    c = _client(session)

    c.put_entity("databaseServices", {"name": "svc"})
    assert session.request.call_args[0][1].endswith("/api/v1/services/databaseServices")

    c.put_entity("tables", {"name": "t"})
    assert session.request.call_args[0][1].endswith("/api/v1/tables")

    c.put_entity("pipelines", {"name": "p"})
    assert session.request.call_args[0][1].endswith("/api/v1/pipelines")

    c.put_entity("pipelineServices", {"name": "ps"})
    assert session.request.call_args[0][1].endswith("/api/v1/services/pipelineServices")


def test_backoff_retries_on_500_then_succeeds():
    session = mock.Mock()
    session.request.side_effect = [
        FakeResponse(500, {"err": "x"}),
        FakeResponse(200, {"id": "1"}),
    ]
    c = _client(session)
    out = c.put_entity("tables", {"name": "t"})
    assert out == {"id": "1"}
    assert session.request.call_count == 2


def test_connection_error_retried_then_raises():
    session = mock.Mock()
    session.request.side_effect = requests.ConnectionError("boom")
    c = OMClient("https://om.example.com", "tok", session=session, backoff_base=0, max_retries=2)
    with pytest.raises(OMClientError):
        c.put_entity("tables", {"name": "t"})
    assert session.request.call_count == 3  # initial + 2 retries


def test_401_raises_userexception():
    session = mock.Mock()
    session.request.return_value = FakeResponse(401, {"error": "unauthorized"})
    c = _client(session)
    with pytest.raises(OMAuthError):
        c.probe_version()


def test_412_raises_precondition():
    session = mock.Mock()
    session.request.return_value = FakeResponse(412, {})
    c = _client(session)
    with pytest.raises(OMPreconditionFailed):
        c.patch_entity("tables", "svc.p.b.t", [{"op": "add", "path": "/x", "value": 1}])


def test_bulk_override_ignored_on_1_13():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {})
    c = _client(session)
    c.server_version = "1.13.4"
    c.bulk_put_tables([{"name": "t"}], override_metadata=True)
    # no overrideMetadata param on 1.13.4
    assert session.request.call_args.kwargs["params"] is None


def test_bulk_override_used_on_2_0():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {})
    c = _client(session)
    c.server_version = "2.0.0"
    assert c.is_2_0_or_newer is True
    c.bulk_put_tables([{"name": "t"}], override_metadata=True)
    assert session.request.call_args.kwargs["params"] == {"overrideMetadata": "true"}


def test_delete_stale_refused_on_1_13():
    session = mock.Mock()
    c = _client(session)
    c.server_version = "1.13.4"
    with pytest.raises(OMClientError):
        c.delete_stale({"scopeFqn": "svc.p"})


def test_delete_lineage_by_name_on_2_0():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {})
    c = _client(session)
    c.server_version = "2.0.0"
    c.delete_lineage_by_source("table", "svc.p.b.t", "PipelineLineage")
    called_path = session.request.call_args[0][1]
    assert "/lineage/source/name/table/" in called_path
    assert "/type/PipelineLineage" in called_path


def test_delete_lineage_by_id_on_1_13():
    session = mock.Mock()
    # first: get_by_fqn -> entity with id ; second: the delete
    session.request.side_effect = [
        FakeResponse(200, {"id": "eid-1"}),
        FakeResponse(200, {}),
    ]
    c = _client(session)
    c.server_version = "1.13.4"
    c.delete_lineage_by_source("table", "svc.p.b.t", "PipelineLineage")
    delete_path = session.request.call_args_list[-1][0][1]
    assert "/lineage/table/eid-1/type/PipelineLineage" in delete_path


def test_patch_uses_json_patch_content_type():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {"id": "1"})
    c = _client(session)
    c.patch_entity("tables", "svc.p.b.t", [{"op": "add", "path": "/description", "value": "x"}])
    headers = session.request.call_args.kwargs["headers"]
    assert headers["Content-Type"] == "application/json-patch+json"


def test_list_paginates_via_cursor():
    session = mock.Mock()
    session.request.side_effect = [
        FakeResponse(200, {"data": [{"id": "1"}], "paging": {"after": "cur1"}}),
        FakeResponse(200, {"data": [{"id": "2"}], "paging": {}}),
    ]
    c = _client(session)
    ids = [e["id"] for e in c.list("tables")]
    assert ids == ["1", "2"]
    assert session.request.call_count == 2
