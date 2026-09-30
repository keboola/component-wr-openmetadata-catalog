from mapping.table_type import EXTERNAL, REGULAR, VIEW, detect_table_type


def _detect(**kw):
    base = {
        "stage": "out",
        "sharing": None,
        "has_external_schema": False,
        "has_source_bucket": False,
        "is_alias": False,
    }
    base.update(kw)
    return detect_table_type(**base)


def test_external_when_external_schema_and_no_sharing():
    assert _detect(stage="in", has_external_schema=True, sharing=None) == EXTERNAL
    assert _detect(stage="in", has_external_schema=True, sharing="none") == EXTERNAL


def test_out_bucket_is_regular():
    assert _detect(stage="out", sharing=None) == REGULAR
    assert _detect(stage="out", sharing="specific-projects") == REGULAR
    assert _detect(stage="shared", sharing="none") == REGULAR
    assert _detect(stage="linked", sharing=None) == REGULAR


def test_in_bucket_with_source_is_view():
    assert _detect(stage="in", has_source_bucket=True) == VIEW


def test_table_alias_is_view():
    assert _detect(stage="in", is_alias=True) == VIEW


def test_default_is_regular():
    assert _detect(stage="in") == REGULAR


def test_external_priority_over_regular():
    # An external-schema bucket in stage out still classifies as External.
    assert _detect(stage="out", has_external_schema=True, sharing=None) == EXTERNAL
