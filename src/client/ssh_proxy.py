"""Optional SSH bastion tunnel around the OM host (spec 3.4, T6).

When ``use_ssh_tunnel`` is enabled a local forward is opened to the OM host
through the bastion before the OM client is built; a tunnel failure is a setup
error (``UserException`` -> exit 1). Disabled -> no-op.
"""

from __future__ import annotations

import io
import logging
from types import TracebackType
from typing import Self
from urllib.parse import urlparse, urlunparse

from keboola.component.exceptions import UserException

logger = logging.getLogger(__name__)

_DEFAULT_HTTPS_PORT = 443
_DEFAULT_HTTP_PORT = 80


def _remote_host_port(om_host: str) -> tuple[str, int]:
    parsed = urlparse(om_host if "://" in om_host else f"https://{om_host}")
    host = parsed.hostname or om_host
    port = parsed.port or (_DEFAULT_HTTP_PORT if parsed.scheme == "http" else _DEFAULT_HTTPS_PORT)
    return host, port


class SshProxy:
    """Context manager wrapping an ``sshtunnel`` forward to the OM host."""

    def __init__(self, ssh_config, om_host: str) -> None:
        self._ssh_config = ssh_config
        self._om_host = om_host
        self._forwarder = None
        self.local_om_host: str | None = None

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def open(self) -> str:
        """Open the tunnel; return the rewritten local OM base URL."""
        try:
            from sshtunnel import SSHTunnelForwarder
        except ImportError as exc:  # pragma: no cover - dependency always present in image
            raise UserException("sshtunnel is not available but use_ssh_tunnel is enabled.") from exc

        remote_host, remote_port = _remote_host_port(self._om_host)
        try:
            self._forwarder = SSHTunnelForwarder(
                (self._ssh_config.host, int(self._ssh_config.port)),
                ssh_username=self._ssh_config.user,
                ssh_pkey=io.StringIO(self._ssh_config.private_key),
                remote_bind_address=(remote_host, remote_port),
            )
            self._forwarder.start()
        except Exception as exc:
            raise UserException(f"Failed to open SSH tunnel to {remote_host}:{remote_port}: {exc}") from exc

        local_port = self._forwarder.local_bind_port
        parsed = urlparse(self._om_host if "://" in self._om_host else f"https://{self._om_host}")
        self.local_om_host = urlunparse(parsed._replace(netloc=f"127.0.0.1:{local_port}"))
        logger.info("SSH tunnel open: OM host %s -> 127.0.0.1:%s", remote_host, local_port)
        return self.local_om_host

    def close(self) -> None:
        if self._forwarder is not None:
            try:
                self._forwarder.stop()
            except Exception:
                logger.debug("SSH tunnel stop raised during teardown", exc_info=True)
            self._forwarder = None


def maybe_open_tunnel(config) -> SshProxy | None:
    """Return an opened :class:`SshProxy` when enabled, else ``None`` (no-op)."""
    if not getattr(config, "use_ssh_tunnel", False):
        return None
    if config.ssh is None:
        raise UserException("use_ssh_tunnel is enabled but the ssh configuration block is missing.")
    proxy = SshProxy(config.ssh, config.om_host)
    proxy.open()
    return proxy
