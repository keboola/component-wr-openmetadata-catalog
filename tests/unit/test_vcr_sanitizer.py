"""Unit tests for the VCR ``CredentialScrubber`` (defense-in-depth secret redaction).

These prove the second sanitizer layer added after the 08/10 leak: a recording of
``GET /v2/storage/.../components?include=configuration`` echoes *every other*
component's configuration, whose secret field names this writer never enumerated.
The scrubber must redact any credential-shaped field name or value recursively,
while preserving non-secret data (SQL blocks, storage mappings, ids). It runs at
record time only, so it cannot alter the already-recorded clean cassettes.
"""

from __future__ import annotations

import json
import types
from typing import Any

from component import VCR_SANITIZERS, CredentialScrubber

# A synthetic ``components?include=configuration`` response: one transformation
# (SQL + storage mappings we must KEEP) and one writer whose parameters carry the
# kind of FOREIGN secrets that leaked into cassettes 08/10 — fake values only.
_FOREIGN_CONFIG_DUMP = [
    {
        "id": "keboola.snowflake-transformation",
        "configurations": [
            {
                "id": "123",
                "configuration": {
                    "parameters": {
                        "blocks": [{"name": "b1", "codes": [{"name": "c1", "script": ["SELECT id, amount FROM in_t"]}]}]
                    },
                    "storage": {
                        "input": {"tables": [{"source": "in.c-sales.orders", "destination": "in_t"}]},
                        "output": {
                            "tables": [{"source": "in_t", "destination": "out.c-sales.summary", "primary_key": ["id"]}]
                        },
                    },
                },
            }
        ],
    },
    {
        "id": "keboola.wr-db-snowflake",
        "configurations": [
            {
                "id": "456",
                "configuration": {
                    "parameters": {
                        "db": {"#password": "s3cr3t-pw", "host": "h.example.com", "port": 443},
                        "private_key": "-----BEGIN RSA PRIVATE KEY-----\nAAAABBBBCCCC\n-----END RSA PRIVATE KEY-----",
                        "SNOWFLAKE_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\nDDDDEEEEFFFF\n-----END PRIVATE KEY-----",
                        "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
                        "aws_access_key_id": "AKIAABCDEFGHIJKLMNOP",
                        "access_key": "plain-access-key-value",
                        "api_key": "plain-api-key-value",
                        "api_key_id": "plain-api-key-id-value",
                        "aws_key_id": "plain-aws-key-id-value",
                        "token": "plain-token-value",
                    }
                },
            }
        ],
    },
]

_REDACTED = CredentialScrubber.REPLACEMENT


def _scrub_response(blob: Any) -> tuple[str, Any]:
    """Run ``blob`` through ``before_record_response`` and return (raw_text, parsed)."""
    response = {"body": {"string": json.dumps(blob)}}
    out = CredentialScrubber().before_record_response(response)
    text = out["body"]["string"]
    return text, json.loads(text)


def test_credential_scrubber_is_wired_after_default_sanitizer():
    """The scrubber is the SECOND VCR sanitizer, so it runs after DefaultSanitizer."""
    assert any(isinstance(s, CredentialScrubber) for s in VCR_SANITIZERS)
    types_ = [type(s).__name__ for s in VCR_SANITIZERS]
    assert types_[0] == "DefaultSanitizer"
    assert "CredentialScrubber" in types_


def test_foreign_private_keys_and_aws_ids_fully_redacted_in_response():
    """No PEM block or AWS access-key id survives anywhere in the recorded body."""
    text, _ = _scrub_response(_FOREIGN_CONFIG_DUMP)
    assert "PRIVATE KEY" not in text
    assert "AKIA" not in text
    assert "s3cr3t-pw" not in text
    assert "plain-api-key-value" not in text
    assert "plain-api-key-id-value" not in text
    assert "plain-aws-key-id-value" not in text
    assert "plain-access-key-value" not in text
    assert "plain-token-value" not in text


def test_every_foreign_secret_field_redacted_by_name():
    """Each foreign credential field — none of which the writer declared — is REDACTED."""
    _, parsed = _scrub_response(_FOREIGN_CONFIG_DUMP)
    params = parsed[1]["configurations"][0]["configuration"]["parameters"]
    for field in (
        "private_key",
        "SNOWFLAKE_PRIVATE_KEY",
        "accessKeyId",
        "aws_access_key_id",
        "access_key",
        "api_key",
        "api_key_id",
        "aws_key_id",
        "token",
    ):
        assert params[field] == _REDACTED, field
    assert params["db"]["#password"] == _REDACTED


def test_non_secret_fields_preserved():
    """SQL blocks, storage mappings, primary_key, ids and hosts are untouched."""
    _, parsed = _scrub_response(_FOREIGN_CONFIG_DUMP)
    transform = parsed[0]["configurations"][0]["configuration"]
    assert transform["parameters"]["blocks"][0]["codes"][0]["script"] == ["SELECT id, amount FROM in_t"]
    assert transform["storage"]["input"]["tables"][0]["source"] == "in.c-sales.orders"
    assert transform["storage"]["output"]["tables"][0]["destination"] == "out.c-sales.summary"
    # primary_key matches *_key but is an explicitly-allowlisted, non-secret field.
    assert transform["storage"]["output"]["tables"][0]["primary_key"] == ["id"]
    writer_db = parsed[1]["configurations"][0]["configuration"]["parameters"]["db"]
    assert writer_db["host"] == "h.example.com"
    assert writer_db["port"] == 443


def test_value_shape_redaction_ignores_field_name():
    """A PEM block or AKIA id under an innocuously-named field is still redacted by shape."""
    _, parsed = _scrub_response(
        {"note": "leaked -----BEGIN PRIVATE KEY-----\nXX\n-----END PRIVATE KEY----- here", "id": "AKIAIOSFODNN7EXAMPLE"}
    )
    assert "PRIVATE KEY" not in parsed["note"]
    assert parsed["note"] == f"leaked {_REDACTED} here"
    # ``id`` is not a credential name, but its value is AWS-key-shaped -> redacted.
    assert parsed["id"] == _REDACTED


def test_request_body_is_scrubbed():
    """The scrubber also redacts credentials in request bodies (not just responses)."""
    body = json.dumps({"private_key": "-----BEGIN PRIVATE KEY-----\nZZ\n-----END PRIVATE KEY-----", "keep": "value"})
    request = types.SimpleNamespace(uri="https://connection.keboola.com/v2/storage/x", headers={}, body=body)
    out = CredentialScrubber().before_record_request(request)
    parsed = json.loads(out.body)
    assert parsed["private_key"] == _REDACTED
    assert parsed["keep"] == "value"


def test_non_json_body_shape_fallback():
    """A non-JSON body still has credential-shaped substrings stripped."""
    raw = "dump AKIAIOSFODNN7EXAMPLE and -----BEGIN PRIVATE KEY-----\nQQ\n-----END PRIVATE KEY----- tail"
    scrubbed = CredentialScrubber.scrub_body_text(raw)
    assert "AKIA" not in scrubbed
    assert "PRIVATE KEY" not in scrubbed
    assert "dump" in scrubbed and "tail" in scrubbed


def test_top_level_json_array_is_walked():
    """A bare JSON array response (list endpoints) is recursed, not skipped."""
    _, parsed = _scrub_response([{"private_key": "-----BEGIN PRIVATE KEY-----\nA\n-----END PRIVATE KEY-----"}])
    assert parsed[0]["private_key"] == _REDACTED


def test_primary_key_is_not_treated_as_credential():
    """The *_key rule must not redact the storage ``primary_key`` list."""
    assert CredentialScrubber._is_credential_name("primary_key") is False
    assert CredentialScrubber._is_credential_name("primaryKey") is False
    assert CredentialScrubber._is_credential_name("private_key") is True
    assert CredentialScrubber._is_credential_name("SNOWFLAKE_PRIVATE_KEY") is True
    assert CredentialScrubber._is_credential_name("accessKeyId") is True
    assert CredentialScrubber._is_credential_name("columns") is False
