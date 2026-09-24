"""Unit coverage for the shared enrichment helpers (``mapping.enrichment``).

These are the single-source implementations of the e-mail regex, the
creator-token extractor, the ``hyperlink-cp`` value shape, and the
``KBC_STACKID`` connection-base logic — every builder that writes native OM
``owners`` plus typed ``extension`` custom properties (dashboard, entity,
pipeline) delegates to this module rather than keeping its own copy.
"""

from mapping import enrichment
from mapping.dashboard_builder import DashboardBuilder

# --------------------------------------------------------------------- identity
# DashboardBuilder._owner is a direct alias of enrichment.creator_token_email,
# not a wrapper — proves the refactor delegates rather than duplicating.


def test_dashboard_owner_uses_shared_email_extractor():
    assert DashboardBuilder._owner is enrichment.creator_token_email


# --------------------------------------------------------------------- extract_email


def test_extract_email_plain():
    assert enrichment.extract_email("jakub.smagin@keboola.com") == "jakub.smagin@keboola.com"


def test_extract_email_wrapped_in_text():
    assert enrichment.extract_email("kbagent-cli [martin.struzsky@keboola.com]") == "martin.struzsky@keboola.com"


def test_extract_email_no_email_returns_none():
    assert enrichment.extract_email("some token label") is None


def test_extract_email_none_input_returns_none():
    assert enrichment.extract_email(None) is None


def test_extract_email_accepts_multichar_tld():
    # A real gTLD (seen in the wild: *.consulting) must still be accepted.
    assert enrichment.extract_email("jiri.soukup@keboola.consulting") == "jiri.soukup@keboola.consulting"


def test_extract_email_strips_trailing_period():
    # "…@keboola.com." (address ends a sentence) must not keep the trailing dot,
    # or OM's email-typed custom property (kbcOwner) 400s -> failed entity.
    assert enrichment.extract_email("owner is owner@keboola.com.") == "owner@keboola.com"


def test_extract_email_ignores_ip_like_host():
    # A numeric "TLD" is not a valid mailbox domain; emit None rather than a 400-bait value.
    assert enrichment.extract_email("host@10.20.30.40") is None


def test_extract_email_skips_leading_dot_local_part():
    # Leading dot in the local part is invalid; the valid sub-address is extracted instead.
    assert enrichment.extract_email(".owner@keboola.com") == "owner@keboola.com"


# --------------------------------------------------------------------- creator_token_email


def test_creator_token_email_from_current_version():
    config = {"currentVersion": {"creatorToken": {"description": "a@keboola.com"}}}
    assert enrichment.creator_token_email(config) == "a@keboola.com"


def test_creator_token_email_falls_back_to_top_level_creator_token():
    config = {"creatorToken": {"description": "b@keboola.com"}}
    assert enrichment.creator_token_email(config) == "b@keboola.com"


def test_creator_token_email_wrapped_description():
    config = {"currentVersion": {"creatorToken": {"description": "kbagent-cli [c@keboola.com]"}}}
    assert enrichment.creator_token_email(config) == "c@keboola.com"


def test_creator_token_email_missing_returns_none():
    assert enrichment.creator_token_email({}) is None


def test_creator_token_email_no_email_in_description_returns_none():
    config = {"currentVersion": {"creatorToken": {"description": "no email here"}}}
    assert enrichment.creator_token_email(config) is None


# --------------------------------------------------------------------- config_last_change


def test_config_last_change_prefers_current_version_created():
    config = {"created": "2020-01-01T00:00:00", "currentVersion": {"created": "2026-04-14T20:56:16+0200"}}
    assert enrichment.config_last_change(config) == "2026-04-14 20:56"


def test_config_last_change_falls_back_to_top_level_created():
    config = {"created": "2020-01-01T09:30:00"}
    assert enrichment.config_last_change(config) == "2020-01-01 09:30"


def test_config_last_change_missing_returns_none():
    assert enrichment.config_last_change({}) is None


# --------------------------------------------------------------------- connection_base


def test_connection_base_builds_from_stack_id():
    assert enrichment.connection_base("us-east4.gcp.keboola.com", "https://fallback") == (
        "https://connection.us-east4.gcp.keboola.com"
    )


def test_connection_base_already_prefixed_stack_id_not_doubled():
    assert enrichment.connection_base("connection.us-east4.gcp.keboola.com", "https://fallback") == (
        "https://connection.us-east4.gcp.keboola.com"
    )


def test_connection_base_falls_back_without_stack_id():
    assert enrichment.connection_base(None, "https://fallback") == "https://fallback"


# --------------------------------------------------------------------- hyperlink


def test_hyperlink_with_url():
    assert enrichment.hyperlink("https://x", "Open") == {"url": "https://x", "displayText": "Open"}


def test_hyperlink_without_url_is_none():
    assert enrichment.hyperlink(None, "Open") is None


# --------------------------------------------------------------------- native_owners


def test_native_owners_resolver_finds_user():
    assert enrichment.native_owners("a@keboola.com", lambda email: "om-user-1") == [{"id": "om-user-1", "type": "user"}]


def test_native_owners_resolver_returns_none_yields_none():
    assert enrichment.native_owners("a@keboola.com", lambda email: None) is None


def test_native_owners_no_email_resolver_never_called():
    calls = []

    def resolver(email):
        calls.append(email)
        return "x"

    assert enrichment.native_owners(None, resolver) is None
    assert calls == []


def test_native_owners_no_resolver_yields_none():
    assert enrichment.native_owners("a@keboola.com", None) is None
