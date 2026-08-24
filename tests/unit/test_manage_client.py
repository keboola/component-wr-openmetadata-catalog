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
    # Enumeration must hit the PATH-SCOPED endpoint for the one org, never the
    # list-all /manage/organizations (which would leak every org's projects).
    first_url = session.request.call_args_list[0][0][1]
    assert first_url.endswith("/manage/organizations/99/projects")
    all_urls = [c[0][1] for c in session.request.call_args_list]
    assert not any(u.endswith("/manage/organizations") for u in all_urls)


def test_enumerate_and_mint_requires_organization_id():
    session = mock.Mock()
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    with pytest.raises(ManageScopeError):
        client.enumerate_and_mint(organization_id=None)
    # No HTTP call is made when the org id is absent.
    session.request.assert_not_called()


def test_scope_403_raises_degrade_signal():
    session = mock.Mock()
    session.request.return_value = FakeResponse(403, {"error": "forbidden"})
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    with pytest.raises(ManageScopeError):
        client.enumerate_and_mint(organization_id="99")


def test_mint_without_token_in_response_raises():
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {})
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)
    with pytest.raises(ManageScopeError):
        client.mint_storage_token("1", "Proj A")
