from unittest import mock

import requests

from client import data_app_reader


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_fetch_app_states_maps_config_to_state():
    payload = [
        {"configId": "abc", "state": "running"},
        {"config_id": "def", "desiredState": "stopped"},
        {"state": "orphan-without-config"},
    ]
    with mock.patch.object(data_app_reader.requests, "get", return_value=_Resp(payload)):
        states = data_app_reader.fetch_app_states("https://data-science.x/", "tok")
    assert states == {"abc": "running", "def": "stopped"}


def test_fetch_app_states_accepts_wrapped_payload():
    with mock.patch.object(
        data_app_reader.requests, "get", return_value=_Resp({"apps": [{"configId": "a", "state": "created"}]})
    ):
        assert data_app_reader.fetch_app_states("https://data-science.x", "tok") == {"a": "created"}


def test_fetch_app_states_empty_on_error():
    with mock.patch.object(data_app_reader.requests, "get", side_effect=requests.RequestException("boom")):
        assert data_app_reader.fetch_app_states("https://data-science.x", "tok") == {}
