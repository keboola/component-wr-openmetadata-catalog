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


def test_unknown_length_varchar_maps_to_text():
    # OM rejects a null dataLength for char/varchar; a length-less varchar must
    # fall back to a length-free OM type (TEXT), never a dataLength:1 placeholder.
    m = map_datatype(definition={"type": "VARCHAR"})
    assert m.data_type == "TEXT"
    assert m.data_length is None
    assert m.data_type_display == "VARCHAR"  # source type name preserved


def test_unknown_length_char_maps_to_text():
    m = map_datatype(definition={"type": "CHAR"})
    assert m.data_type == "TEXT"
    assert m.data_length is None


def test_unknown_length_string_basetype_maps_to_text():
    # A length-less Keboola STRING basetype (no native type) also becomes TEXT.
    m = map_datatype(legacy={"basetype": "STRING"})
    assert m.data_type == "TEXT"
    assert m.data_length is None


def test_known_length_varchar_keeps_varchar_and_length():
    m = map_datatype(definition={"type": "VARCHAR", "length": "255"})
    assert m.data_type == "VARCHAR"
    assert m.data_length == 255


def test_unknown_length_binary_maps_to_bytes():
    # binary/varbinary also require dataLength in OM; length-less -> BYTES.
    m = map_datatype(definition={"type": "BINARY"})
    assert m.data_type == "BYTES"
    assert m.data_length is None


def test_unknown_length_varbinary_maps_to_bytes():
    m = map_datatype(definition={"type": "VARBINARY"})
    assert m.data_type == "BYTES"
    assert m.data_length is None


def test_known_length_binary_keeps_binary_and_length():
    m = map_datatype(definition={"type": "BINARY", "length": "16"})
    assert m.data_type == "BINARY"
    assert m.data_length == 16


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
