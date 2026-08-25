import json
from unittest import mock

import pytest

from client.manage_client import _MINTED_TOKEN_EXPIRES_SECONDS, ManageClient, ManageScopeError
from client.storage_reader import StorageReader


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


def _mint_body(session):
    """Return the JSON body of the POST .../tokens request captured on ``session``."""
    for args, kwargs in session.request.call_args_list:
        if args[0] == "POST" and args[1].endswith("/tokens"):
            return kwargs["json"]
    raise AssertionError("no mint POST was made")


def test_mint_body_grants_bucket_read():
    """BUG #5 regression: the mint body must grant the minted token visibility of
    the project's buckets. Verified against the real Management API, the ONLY
    lever for a freshly minted token to see all buckets is ``canManageBuckets``;
    the previous ``False`` yielded ``bucketPermissions: {}`` and 0 buckets, so
    Tier-2 catalogued nothing. Fails before the fix (was ``False``)."""
    session = mock.Mock()
    session.request.return_value = FakeResponse(200, {"token": "minted-a"})
    client = ManageClient("https://connection.keboola.com", "manage-tok", session=session, backoff_base=0)

    client.mint_storage_token("1", "Proj A")

    body = _mint_body(session)
    assert body["canManageBuckets"] is True
    # Kept minimal / least-privilege: no file-staging access, no trash purge,
    # and a short lifetime bounds the blast radius of the manage-buckets grant.
    assert body["canReadAllFileUploads"] is False
    assert "canPurgeTrash" not in body
    assert isinstance(body["expiresIn"], int)
    assert 0 < body["expiresIn"] <= _MINTED_TOKEN_EXPIRES_SECONDS


class _KeboolaBackend:
    """Models the verified Management+Storage API contract in one fake: the mint
    body's ``canManageBuckets`` flag decides whether the resulting Storage token
    can list the project's buckets (True -> all buckets; False -> empty)."""

    def __init__(self, buckets):
        self._buckets = buckets
        self.token_grants_bucket_read: bool | None = None

    def manage_request(self, method, url, json=None, timeout=None):
        if method == "GET" and url.endswith("/projects"):
            return FakeResponse(200, [{"id": 1, "name": "Proj A"}])
        if method == "POST" and url.endswith("/tokens"):
            self.token_grants_bucket_read = bool((json or {}).get("canManageBuckets"))
            return FakeResponse(200, {"token": "minted-a"})
        raise AssertionError(f"unexpected manage call: {method} {url}")

    def storage_get(self, url, params=None, timeout=None):
        if url.endswith("/v2/storage/buckets"):
            visible = self._buckets if self.token_grants_bucket_read else []
            return FakeResponse(200, visible)
        return FakeResponse(200, {})


def test_minted_token_driven_bucket_list_is_non_empty():
    """End-to-end mocked path: a token minted by the fixed client, when used to
    drive a Storage bucket listing, returns a NON-EMPTY list. Under the old body
    (``canManageBuckets: False``) the same modelled backend would return 0
    buckets, so this test fails before the fix and passes after."""
    backend = _KeboolaBackend(buckets=[{"id": "in.c-main", "name": "main"}])

    manage_session = mock.Mock()
    manage_session.request.side_effect = backend.manage_request
    manage_client = ManageClient(
        "https://connection.keboola.com", "manage-tok", session=manage_session, backoff_base=0
    )

    minted = manage_client.enumerate_and_mint(organization_id="99")
    assert [m.project_id for m in minted] == ["1"]
    assert backend.token_grants_bucket_read is True  # the fixed mint body enabled it

    storage_session = mock.Mock()
    storage_session.get.side_effect = backend.storage_get
    reader = StorageReader(
        minted[0].storage_url, minted[0].storage_token, production_only=False, session=storage_session
    )

    buckets = reader.list_buckets()
    assert [b.id for b in buckets] == ["in.c-main"]
    assert buckets, "Tier-2 must catalog a non-empty bucket list for the minted token"
