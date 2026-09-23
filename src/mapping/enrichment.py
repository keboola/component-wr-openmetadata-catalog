"""Shared enrichment helpers for every builder that writes native OM ``owners``
plus typed ``extension`` custom properties (``dashboard_builder``,
``pipeline_builder``, ``entity_builder``).

Kept in one place so the owner e-mail extractor, the OM ``hyperlink-cp`` value
shape, and the ``KBC_STACKID`` public connection-base URL are each implemented
exactly once — every builder that needs one of these imports it from here
rather than keeping its own copy.
"""

from __future__ import annotations

import re
from collections.abc import Callable

# Deliberately strict so the extracted address is one OpenMetadata's ``email``-typed
# custom properties (kbcOwner) accept under its RFC 5321 mailbox validation: the
# local part is dot-separated segments (no leading / trailing / consecutive dots),
# and the domain must end in a 2+-letter alphabetic TLD. This is what stops a
# creator-token description like ``"see owner@keboola.com."`` (trailing period) or a
# bare ``"host@1.2.3.4"`` from yielding an address OM rejects with a 400 — which,
# under the default ``failure_mode=collect_and_fail``, would fail the whole job.
_EMAIL = re.compile(r"[\w+-]+(?:\.[\w+-]+)*@[\w-]+(?:\.[\w-]+)*\.[A-Za-z]{2,}")


def extract_email(text: str | None) -> str | None:
    """First valid e-mail-shaped substring in ``text``, or ``None``.

    Some sources wrap the address in surrounding text (a creator-token
    description like ``"kbagent-cli [martin@keboola.com]"``); every custom
    property that carries an owner e-mail is ``email``-typed, so the free text
    around the address must be stripped rather than passed through raw, and an
    address OM would reject (trailing punctuation, a numeric "TLD", leading /
    consecutive dots) must not be emitted at all -- ``None`` is safer than a
    value that 400s.
    """
    if not text:
        return None
    match = _EMAIL.search(text)
    return match.group(0) if match else None


def creator_token_email(config: dict) -> str | None:
    """Owner e-mail from a Keboola config/flow's creator-token description.

    Shared by every builder that catalogs a Keboola *configuration* (data
    apps, component configs, flows): the owner is the last editor of the
    config, recorded in ``currentVersion.creatorToken.description`` (falling
    back to the top-level ``creatorToken`` some API responses carry instead).
    """
    version = config.get("currentVersion") or {}
    token = version.get("creatorToken") or config.get("creatorToken") or {}
    return extract_email(token.get("description"))


def config_last_change(config: dict) -> str | None:
    """Last-change timestamp (minute precision) for a Keboola config/flow.

    Prefers ``currentVersion.created`` (the last edit) over the top-level
    ``created`` (the config's original creation date).
    """
    version = config.get("currentVersion") or {}
    timestamp = version.get("created") or config.get("created")
    return timestamp.replace("T", " ")[:16] if timestamp else None


def connection_base(stack_id: str | None, fallback_ui_base: str) -> str:
    """Public Keboola connection base URL from ``KBC_STACKID``.

    URLs built from this base are correct even when the job itself runs
    on-platform behind an internal ``KBC_URL`` — falls back to
    ``fallback_ui_base`` (the internal URL) only when no ``stack_id`` is
    available (e.g. a unit test or a run with no injected environment).
    """
    if stack_id:
        host = stack_id if stack_id.startswith("connection.") else f"connection.{stack_id}"
        return f"https://{host}"
    return fallback_ui_base


def hyperlink(url: str | None, display_text: str) -> dict | None:
    """OM ``hyperlink-cp`` custom-property value, or ``None`` when there is no URL."""
    return {"url": url, "displayText": display_text} if url else None


def native_owners(email: str | None, owner_resolver: Callable[[str], str | None] | None) -> list[dict] | None:
    """Native OM ``owners`` for ``email``, resolved to an OM user id (best-effort).

    Returns ``None`` when there is no e-mail, no resolver, or the resolver
    finds no matching OM user — owner assignment is always additive: the
    e-mail typically still lives in the entity's own ``email``-typed custom
    property (e.g. ``kbcOwner``) regardless of whether a native OM user was
    found. The resolver is never called when there is no e-mail to resolve.
    """
    if not email or owner_resolver is None:
        return None
    owner_id = owner_resolver(email)
    return [{"id": owner_id, "type": "user"}] if owner_id else None
