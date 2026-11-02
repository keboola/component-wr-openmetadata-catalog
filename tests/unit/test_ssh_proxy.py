from unittest import mock

import pytest
from keboola.component.exceptions import UserException
from requests.adapters import HTTPAdapter
from urllib3 import HTTPSConnectionPool

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


def test_sshtunnel_works_with_the_locked_paramiko():
    """sshtunnel 0.4.0 reads paramiko.DSSKey while it builds the forwarder; paramiko 4 removed it.

    Building the forwarder (no connection yet) fails with AttributeError on an
    incompatible paramiko, so this catches a dependency bump that breaks every tunnel.
    """
    import io

    import paramiko
    from sshtunnel import SSHTunnelForwarder

    key = paramiko.RSAKey.generate(2048)
    pem = io.StringIO()
    key.write_private_key(pem)
    forwarder = SSHTunnelForwarder(
        ("127.0.0.1", 2222),
        ssh_username="kbc",
        ssh_pkey=io.StringIO(pem.getvalue()),
        remote_bind_address=("om.example.com", 443),
    )
    assert forwarder.ssh_pkeys


def test_tunnel_session_speaks_tls_to_the_real_host():
    """Through the tunnel the URL is 127.0.0.1, but SNI, certificate check and Host name the OM host."""
    proxy = SshProxy(mock.Mock(), "https://om.example.com")
    proxy.local_om_host = "https://127.0.0.1:40000"
    proxy._remote_netloc = "om.example.com"

    session = proxy.session()

    adapter = session.get_adapter("https://127.0.0.1:40000/api/v1/system/version")
    assert isinstance(adapter, HTTPAdapter)
    pool = adapter.poolmanager.connection_from_url("https://127.0.0.1:40000")
    assert pool.conn_kw["server_hostname"] == "om.example.com"
    assert isinstance(pool, HTTPSConnectionPool)
    assert pool.assert_hostname == "om.example.com"
    assert session.headers["Host"] == "om.example.com"
    assert session.verify is True


def test_tunnel_session_keeps_a_non_default_port_in_the_host_header():
    proxy = SshProxy(mock.Mock(), "https://om.example.com:8585")
    proxy.local_om_host = "https://127.0.0.1:40000"
    proxy._remote_netloc = "om.example.com:8585"
    assert proxy.session().headers["Host"] == "om.example.com:8585"
