from unittest import mock

import pytest
from keboola.component.exceptions import UserException

from client.ssh_proxy import SshProxy, _remote_host_port, maybe_open_tunnel


class _Cfg:
    def __init__(self, use_ssh_tunnel, ssh=None):
        self.use_ssh_tunnel = use_ssh_tunnel
        self.ssh = ssh
        self.om_host = "https://om.example.com"


class _Ssh:
    host = "bastion"
    user = "kbc"
    port = 22
    private_key = "PKEY"


def test_disabled_is_noop():
    assert maybe_open_tunnel(_Cfg(False)) is None


def test_remote_host_port_defaults():
    assert _remote_host_port("https://om.example.com") == ("om.example.com", 443)
    assert _remote_host_port("http://om.example.com") == ("om.example.com", 80)
    assert _remote_host_port("https://om.example.com:8585") == ("om.example.com", 8585)


def test_failing_tunnel_raises_userexception():
    proxy = SshProxy(_Ssh(), "https://om.example.com")
    forwarder_cls = mock.Mock(side_effect=RuntimeError("no route to host"))
    fake_module = mock.Mock(SSHTunnelForwarder=forwarder_cls)
    with mock.patch.dict("sys.modules", {"sshtunnel": fake_module}), pytest.raises(UserException):
        proxy.open()


def test_successful_tunnel_rewrites_host():
    fake_forwarder = mock.Mock()
    fake_forwarder.local_bind_port = 12345
    forwarder_cls = mock.Mock(return_value=fake_forwarder)
    with mock.patch.dict("sys.modules", {"sshtunnel": mock.Mock(SSHTunnelForwarder=forwarder_cls)}):
        proxy = SshProxy(_Ssh(), "https://om.example.com")
        local = proxy.open()
        assert local == "https://127.0.0.1:12345"
        proxy.close()
        fake_forwarder.start.assert_called_once()
        fake_forwarder.stop.assert_called_once()
