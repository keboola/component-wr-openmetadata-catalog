import json
from unittest import mock

import pytest
from keboola.component.exceptions import UserException

from client.storage_reader import StorageReader, resolve_storage_credentials


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


def _reader(routes):
    """routes: dict path -> body. Returns a StorageReader over a mocked session."""
    session = mock.Mock()

    def _get(url, params=None, timeout=None):
        path = url.replace("https://connection.keboola.com", "")
        for known, body in routes.items():
            if path == known:
                return FakeResponse(200, body)
        return FakeResponse(404, {})

    session.get.side_effect = _get
    return StorageReader("https://connection.keboola.com", "tok", session=session)


def test_resolve_credentials_prefers_minted():
    tok, url = resolve_storage_credentials(
        row_token="row",
        injected_token="inj",
        injected_url="https://x",
        minted_token="minted",
        minted_url="https://m/",
    )
    assert tok == "minted"
    assert url == "https://m"


def test_resolve_credentials_row_then_forward():
    tok, url = resolve_storage_credentials(row_token="row", injected_token=None, injected_url="https://host/")
    assert (tok, url) == ("row", "https://host")

    tok2, url2 = resolve_storage_credentials(row_token=None, injected_token="inj", injected_url="https://host/")
    assert (tok2, url2) == ("inj", "https://host")


def test_resolve_credentials_missing_raises():
    with pytest.raises(UserException):
        resolve_storage_credentials(row_token=None, injected_token=None, injected_url=None)


def test_list_buckets_parses_and_filters_dev_branch():
    routes = {
        "/v2/storage/buckets": [
            {
                "id": "out.c-sales",
                "name": "c-sales",
                "stage": "out",
                "path": "out.c-sales",
                "metadata": [{"key": "KBC.description", "value": "Sales bucket"}],
            },
            {
                "id": "out.c-dev",
                "name": "c-dev",
                "stage": "out",
                "metadata": [{"key": "KBC.createdBy.branch.id", "value": "456"}],
            },
        ]
    }
    reader = _reader(routes)
    buckets = reader.list_buckets()
    assert len(buckets) == 1  # dev-branch bucket filtered out
    assert buckets[0].id == "out.c-sales"
    assert buckets[0].description == "Sales bucket"


def test_list_buckets_includes_dev_when_all_branches():
    routes = {
        "/v2/storage/buckets": [
            {
                "id": "out.c-dev",
                "name": "c-dev",
                "stage": "out",
                "metadata": [{"key": "KBC.createdBy.branch.id", "value": "456"}],
            },
        ]
    }
    session = mock.Mock()
    session.get.return_value = FakeResponse(200, routes["/v2/storage/buckets"])
    reader = StorageReader("https://connection.keboola.com", "t", production_only=False, session=session)
    assert len(reader.list_buckets()) == 1


def test_get_table_typed_and_legacy_columns_and_pk():
    body = {
        "id": "out.c-sales.orders",
        "name": "orders",
        "primaryKey": ["id"],
        "columns": ["id", "amount", "note"],
        "definition": {
            "columns": [
                {"name": "id", "definition": {"type": "NUMBER", "length": "38,0"}},
                {"name": "amount", "definition": {"type": "FLOAT"}},
            ]
        },
        "columnMetadata": {
            "note": [
                {"key": "KBC.datatype.basetype", "value": "STRING"},
                {"key": "KBC.description", "value": "free text"},
            ]
        },
        "metadata": [{"key": "KBC.description", "value": "Orders table"}],
        "rowsCount": 10,
    }
    session = mock.Mock()
    session.get.return_value = FakeResponse(200, body)
    reader = StorageReader("https://connection.keboola.com", "t", session=session)
    table = reader.get_table("out.c-sales.orders")
    assert table.primary_key == ["id"]
    assert table.description == "Orders table"
    cols = {c.name: c for c in table.columns}
    assert cols["id"].definition == {"type": "NUMBER", "length": "38,0"}
    assert cols["note"].legacy == {"basetype": "STRING"}
    assert cols["note"].description == "free text"


def test_missing_token_401_raises_userexception():
    session = mock.Mock()
    session.get.return_value = FakeResponse(401, {})
    reader = StorageReader("https://connection.keboola.com", "bad", session=session)
    with pytest.raises(UserException):
        reader.verify_token()


class CsvResponse:
    def __init__(self, text, status_code=200):
        self.status_code = status_code
        self.text = text


def _snapshot_reader(csv_text):
    session = mock.Mock()
    session.get.return_value = CsvResponse(csv_text)
    return StorageReader("https://connection.keboola.com", "tok", session=session)


def test_read_snapshot_rows_warns_when_data_preview_row_cap_hit(caplog):
    """When the returned row count hits the requested limit the merge base is
    likely truncated, so a clear warning is emitted (mitigation, not a fix)."""
    csv_text = "kind,fqn\na,1\nb,2\n"  # 2 data rows; request limit=2 -> cap hit
    reader = _snapshot_reader(csv_text)
    with caplog.at_level("WARNING"):
        rows = reader.read_snapshot_rows("in.c-x.snapshot", limit=2)
    assert len(rows) == 2
    assert any("truncated" in r.message.lower() or "row cap" in r.message.lower() for r in caplog.records)


def test_read_snapshot_rows_no_warning_below_cap(caplog):
    csv_text = "kind,fqn\na,1\n"  # 1 data row, well under the limit
    reader = _snapshot_reader(csv_text)
    with caplog.at_level("WARNING"):
        rows = reader.read_snapshot_rows("in.c-x.snapshot", limit=1000)
    assert len(rows) == 1
    assert caplog.records == []
