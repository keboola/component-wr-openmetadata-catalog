import json
from unittest import mock

import pytest

from client.manage_client import ManageClient, ManageScopeError


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.text = json.dumps(self._body)
        self.content = self.text.encode()

    def json(self):
        return self._body


def test_enumerate_and_mint_happy_path():
    session = mock.Mock()
    session.request.side_effect = [
        FakeResponse(200, [{"id": 1, "name": "Proj A"}, {"id": 2, "name": "Proj B"}]),
        FakeResponse(200, {"token": "minted-a"}),
        FakeResponse(200, {"token": "minted-b"}),
    ]
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    minted = client.enumerate_and_mint(organization_id="99")
    assert [m.project_id for m in minted] == ["1", "2"]
    assert [m.storage_token for m in minted] == ["minted-a", "minted-b"]
    assert minted[0].storage_url == "https://connection.keboola.com"
    assert minted[0].project_name == "Proj A"


def test_scope_403_raises_degrade_signal():
    session = mock.Mock()
    session.request.return_value = FakeResponse(403, {"error": "forbidden"})
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    with pytest.raises(ManageScopeError):
        client.enumerate_and_mint(organization_id="99")


def test_resolve_org_id_from_single_org():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, [{"id": 42}])
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    assert client.resolve_organization_id(None) == "42"


def test_resolve_org_id_ambiguous_raises():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, [{"id": 1}, {"id": 2}])
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    with pytest.raises(ManageScopeError):
        client.resolve_organization_id(None)


def test_mint_without_token_in_response_raises():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {})
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    with pytest.raises(ManageScopeError):
        client.mint_storage_token("1", "Proj A")
