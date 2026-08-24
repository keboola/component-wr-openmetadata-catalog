"""Keboola native / legacy datatypes -> OpenMetadata ``DataType`` (spec 6.5).

Map order, driven purely off what the Storage API returns for the column
(independent of ``KBC_DATA_TYPE_SUPPORT``):

    typed ``definition.type`` -> typed ``definition.basetype`` -> legacy
    ``KBC.datatype.type`` -> legacy ``KBC.datatype.basetype`` -> UNKNOWN

OpenMetadata rejects a null ``dataLength`` for the ``char``/``varchar``/``binary``/
``varbinary`` data types (``400 ... dataLength must not be null``). When the source
column carries no usable length, we therefore map it to a length-free OM type
(``TEXT`` for the char family, ``BYTES`` for the binary family) rather than emit a
fake ``dataLength: 1`` placeholder (spec 6.5 / E8). A known length keeps
``VARCHAR(n)`` / ``BINARY(n)`` with its ``dataLength``. The mapping always preserves
the original source type name in ``dataTypeDisplay``.
"""

from __future__ import annotations

from dataclasses import dataclass

UNKNOWN = "UNKNOWN"

# OM DataType values that carry a length; we only ever set dataLength for these.
# OM also *requires* a non-null dataLength for exactly these types.
_LENGTH_TYPES = frozenset({"VARCHAR", "CHAR", "BINARY", "VARBINARY"})

# When a length-required OM type has no usable source length, fall back to a
# length-free OM DataType that OM accepts without a dataLength (never a placeholder).
_LENGTHLESS_FALLBACK = {
    "VARCHAR": "TEXT",
    "CHAR": "TEXT",
    "BINARY": "BYTES",
    "VARBINARY": "BYTES",
}

# Keboola basetype (typed definition.basetype or legacy KBC.datatype.basetype) -> OM DataType.
_BASETYPE_TO_OM = {
    "STRING": "VARCHAR",
    "INTEGER": "INT",
    "NUMERIC": "NUMERIC",
    "FLOAT": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
}

# Native backend type name (typed definition.type or legacy KBC.datatype.type) -> OM DataType.
_NATIVE_TO_OM = {
    "INT": "INT",
    "INTEGER": "INT",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",
    "BYTEINT": "TINYINT",
    "NUMBER": "NUMERIC",
    "NUMERIC": "NUMERIC",
    "DECIMAL": "DECIMAL",
    "FLOAT": "FLOAT",
    "FLOAT4": "FLOAT",
    "FLOAT8": "DOUBLE",
    "REAL": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DOUBLE PRECISION": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "BIT": "BOOLEAN",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "DATETIME2": "DATETIME",
    "SMALLDATETIME": "DATETIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "TIMESTAMP_LTZ": "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMPZ",
    "TIMESTAMPTZ": "TIMESTAMPZ",
    "TIME": "TIME",
    "VARCHAR": "VARCHAR",
    "STRING": "VARCHAR",
    "TEXT": "TEXT",
    "NVARCHAR": "VARCHAR",
    "NTEXT": "TEXT",
    "CHAR": "CHAR",
    "CHARACTER": "CHAR",
    "NCHAR": "CHAR",
    "JSON": "JSON",
    "VARIANT": "JSON",
    "OBJECT": "JSON",
    "ARRAY": "ARRAY",
    "BINARY": "BINARY",
    "VARBINARY": "VARBINARY",
    "BYTES": "BINARY",
    "BLOB": "BLOB",
    "GEOGRAPHY": "GEOGRAPHY",
    "GEOMETRY": "GEOMETRY",
}


@dataclass(frozen=True)
class MappedType:
    """The resolved OM column type facets."""

    data_type: str
    data_length: int | None = None
    data_type_display: str | None = None
    array_data_type: str | None = None


def _parse_length(raw: object) -> int | None:
    """Parse a positive integer length from ``"255"`` or ``"38,0"``; else None."""
    if raw is None:
        return None
    head = str(raw).split(",")[0].strip()
    try:
        value = int(head)
    except ValueError, TypeError:
        return None
    return value if value > 0 else None


def _lookup(native: str | None, basetype: str | None) -> str | None:
    if native:
        hit = _NATIVE_TO_OM.get(native.strip().upper())
        if hit:
            return hit
    if basetype:
        hit = _BASETYPE_TO_OM.get(basetype.strip().upper())
        if hit:
            return hit
    return None


def map_datatype(
    *,
    definition: dict | None = None,
    basetype: str | None = None,
    legacy: dict | None = None,
) -> MappedType:
    """Resolve a source column's type facets into OM shape.

    Args:
        definition: the typed Storage ``definition`` dict (``type``, ``length``,
            optionally ``basetype``) when the table carries native types.
        basetype: a column-level ``basetype`` when present.
        legacy: the legacy ``KBC.datatype.*`` values as a dict with keys
            ``type`` / ``basetype`` / ``length``.
    """
    legacy = legacy or {}
    native_name: str | None = None
    raw_length: object = None
    om_type: str | None = None

    if definition:
        native_name = definition.get("type")
        raw_length = definition.get("length")
        om_type = _lookup(native_name, definition.get("basetype") or basetype)

    if om_type is None and (legacy.get("type") or legacy.get("basetype")):
        native_name = legacy.get("type") or native_name
        raw_length = legacy.get("length") if raw_length is None else raw_length
        om_type = _lookup(legacy.get("type"), legacy.get("basetype"))

    if om_type is None and basetype:
        om_type = _lookup(None, basetype)

    if om_type is None:
        om_type = UNKNOWN

    length = _parse_length(raw_length)

    # OM requires a non-null dataLength for char/varchar/binary/varbinary. With no
    # usable source length, remap to a length-free OM type instead of a placeholder.
    # This only fires when length is None, so the known-length display suffix below
    # (which requires a length) is unaffected and keeps the original OM type.
    if length is None and om_type in _LENGTH_TYPES:
        om_type = _LENGTHLESS_FALLBACK[om_type]

    data_length = length if om_type in _LENGTH_TYPES else None

    display = None
    if native_name:
        display = native_name.strip()
        if length is not None and om_type in _LENGTH_TYPES:
            display = f"{display}({length})"

    array_type = None
    if om_type == "ARRAY":
        array_type = "STRING"

    return MappedType(
        data_type=om_type,
        data_length=data_length,
        data_type_display=display,
        array_data_type=array_type,
    )
