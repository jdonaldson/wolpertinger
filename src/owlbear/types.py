"""Shared Presto-family type conversion for Athena and Trino backends."""

import pyarrow as pa


def _find_matching_bracket(s: str, start: int) -> int:
    """Find the index of the '>' that closes the '<' at position start."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "<":
            depth += 1
        elif s[i] == ">":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError(f"Unmatched '<' in type string: {s}")


def _split_top_level_comma(s: str) -> list[str]:
    """Split a string on commas that are not nested inside angle brackets."""
    parts = []
    depth = 0
    current: list[str] = []
    for ch in s:
        if ch == "<":
            depth += 1
            current.append(ch)
        elif ch == ">":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    parts.append("".join(current).strip())
    return parts


def presto_type_to_pyarrow(presto_type: str) -> pa.DataType:
    """Convert a Presto/Trino/Athena type name to a PyArrow data type."""
    presto_type = presto_type.lower().strip()

    if presto_type in ["boolean", "bool"]:
        return pa.bool_()
    elif presto_type == "tinyint":
        return pa.int8()
    elif presto_type == "smallint":
        return pa.int16()
    elif presto_type in ["int", "integer"]:
        return pa.int32()
    elif presto_type in ["bigint", "long"]:
        return pa.int64()
    elif presto_type in ["float", "real"]:
        return pa.float32()
    elif presto_type in ["double", "double precision"]:
        return pa.float64()
    elif presto_type == "date":
        return pa.date32()
    elif presto_type in ["time", "time with time zone"]:
        return pa.time64("us")
    elif presto_type == "timestamp with time zone":
        return pa.timestamp("us", tz="UTC")
    elif presto_type.startswith("timestamp"):
        return pa.timestamp("us")
    elif presto_type == "interval day to second":
        return pa.duration("us")
    elif presto_type == "interval year to month":
        return pa.month_day_nano_interval()
    elif presto_type.startswith("decimal") or presto_type.startswith("numeric"):
        if "(" in presto_type:
            params = presto_type.split("(")[1].split(")")[0].split(",")
            precision = int(params[0])
            scale = int(params[1]) if len(params) > 1 else 0
            return pa.decimal128(precision, scale)
        return pa.decimal128(38, 18)
    elif presto_type.startswith("varchar") or presto_type.startswith("char"):
        return pa.string()
    elif presto_type in ["string", "text"]:
        return pa.string()
    elif presto_type in ["varbinary", "binary"]:
        return pa.binary()
    elif presto_type.startswith("array"):
        open_bracket = presto_type.index("<")
        close_bracket = _find_matching_bracket(presto_type, open_bracket)
        element_type_str = presto_type[open_bracket + 1 : close_bracket]
        element_type = presto_type_to_pyarrow(element_type_str)
        return pa.list_(element_type)
    elif presto_type.startswith("map"):
        open_bracket = presto_type.index("<")
        close_bracket = _find_matching_bracket(presto_type, open_bracket)
        inner = presto_type[open_bracket + 1 : close_bracket]
        parts = _split_top_level_comma(inner)
        key_type = presto_type_to_pyarrow(parts[0])
        value_type = presto_type_to_pyarrow(parts[1])
        return pa.map_(key_type, value_type)
    else:
        return pa.string()
