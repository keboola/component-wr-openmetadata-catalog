from mapping.datatype import UNKNOWN, map_datatype


def test_typed_definition_wins_over_legacy():
    m = map_datatype(
        definition={"type": "NUMBER", "length": "38,0"},
        legacy={"basetype": "STRING"},
    )
    assert m.data_type == "NUMERIC"
    assert m.data_type_display == "NUMBER"


def test_typed_varchar_carries_length():
    m = map_datatype(definition={"type": "VARCHAR", "length": "255"})
    assert m.data_type == "VARCHAR"
    assert m.data_length == 255
    assert m.data_type_display == "VARCHAR(255)"


def test_legacy_fallback_when_no_definition():
    m = map_datatype(legacy={"type": "TIMESTAMP_NTZ"})
    assert m.data_type == "TIMESTAMP"


def test_legacy_basetype_fallback():
    m = map_datatype(legacy={"basetype": "INTEGER"})
    assert m.data_type == "INT"


def test_unknown_last():
    m = map_datatype(definition={"type": "SOME_WEIRD_TYPE"})
    assert m.data_type == UNKNOWN


def test_unknown_length_varchar_omits_datalength():
    # Never emit the dataLength:1 placeholder for an unknown-length char type.
    m = map_datatype(definition={"type": "VARCHAR"})
    assert m.data_type == "VARCHAR"
    assert m.data_length is None
    assert m.data_type_display == "VARCHAR"


def test_length_only_set_for_char_types():
    m = map_datatype(definition={"type": "NUMBER", "length": "38"})
    assert m.data_length is None  # NUMERIC does not carry OM dataLength


def test_mapping_independent_of_data_type_support(monkeypatch):
    # KBC_DATA_TYPE_SUPPORT must not influence the source-read mapping.
    monkeypatch.setenv("KBC_DATA_TYPE_SUPPORT", "none")
    a = map_datatype(definition={"type": "VARCHAR", "length": "10"})
    monkeypatch.delenv("KBC_DATA_TYPE_SUPPORT", raising=False)
    b = map_datatype(definition={"type": "VARCHAR", "length": "10"})
    assert a == b


def test_array_type_defaults_to_string():
    m = map_datatype(definition={"type": "ARRAY"})
    assert m.data_type == "ARRAY"
    assert m.array_data_type == "STRING"
