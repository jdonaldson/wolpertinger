"""Tests for presto_type_to_pyarrow type conversion."""

import pytest
import pyarrow as pa

from owlbear.types import presto_type_to_pyarrow


class TestBasicTypes:
    def test_boolean(self):
        assert presto_type_to_pyarrow("boolean") == pa.bool_()
        assert presto_type_to_pyarrow("bool") == pa.bool_()
        assert presto_type_to_pyarrow("BOOLEAN") == pa.bool_()

    def test_tinyint(self):
        assert presto_type_to_pyarrow("tinyint") == pa.int8()

    def test_smallint(self):
        assert presto_type_to_pyarrow("smallint") == pa.int16()

    def test_integer(self):
        assert presto_type_to_pyarrow("int") == pa.int32()
        assert presto_type_to_pyarrow("integer") == pa.int32()
        assert presto_type_to_pyarrow("INTEGER") == pa.int32()

    def test_bigint(self):
        assert presto_type_to_pyarrow("bigint") == pa.int64()
        assert presto_type_to_pyarrow("long") == pa.int64()

    def test_float(self):
        assert presto_type_to_pyarrow("float") == pa.float32()
        assert presto_type_to_pyarrow("real") == pa.float32()

    def test_double(self):
        assert presto_type_to_pyarrow("double") == pa.float64()
        assert presto_type_to_pyarrow("double precision") == pa.float64()

    def test_date(self):
        assert presto_type_to_pyarrow("date") == pa.date32()

    def test_timestamp(self):
        assert presto_type_to_pyarrow("timestamp") == pa.timestamp("us")

    def test_timestamp_with_time_zone(self):
        assert presto_type_to_pyarrow("timestamp with time zone") == pa.timestamp("us", tz="UTC")

    def test_time(self):
        assert presto_type_to_pyarrow("time") == pa.time64("us")
        assert presto_type_to_pyarrow("time with time zone") == pa.time64("us")

    def test_interval(self):
        assert presto_type_to_pyarrow("interval day to second") == pa.duration("us")
        assert presto_type_to_pyarrow("interval year to month") == pa.month_day_nano_interval()


class TestDecimalTypes:
    def test_decimal_with_params(self):
        assert presto_type_to_pyarrow("decimal(10,2)") == pa.decimal128(10, 2)

    def test_decimal_precision_only(self):
        assert presto_type_to_pyarrow("decimal(10)") == pa.decimal128(10, 0)

    def test_decimal_default(self):
        assert presto_type_to_pyarrow("decimal") == pa.decimal128(38, 18)

    def test_numeric(self):
        assert presto_type_to_pyarrow("numeric(5,3)") == pa.decimal128(5, 3)


class TestStringTypes:
    def test_varchar(self):
        assert presto_type_to_pyarrow("varchar") == pa.string()
        assert presto_type_to_pyarrow("varchar(255)") == pa.string()

    def test_char(self):
        assert presto_type_to_pyarrow("char") == pa.string()
        assert presto_type_to_pyarrow("char(10)") == pa.string()

    def test_string(self):
        assert presto_type_to_pyarrow("string") == pa.string()

    def test_text(self):
        assert presto_type_to_pyarrow("text") == pa.string()


class TestBinaryTypes:
    def test_varbinary(self):
        assert presto_type_to_pyarrow("varbinary") == pa.binary()

    def test_binary(self):
        assert presto_type_to_pyarrow("binary") == pa.binary()


class TestComplexTypes:
    def test_array_of_integers(self):
        result = presto_type_to_pyarrow("array<integer>")
        assert result == pa.list_(pa.int32())

    def test_array_of_strings(self):
        result = presto_type_to_pyarrow("array<varchar>")
        assert result == pa.list_(pa.string())

    def test_nested_array(self):
        result = presto_type_to_pyarrow("array<array<integer>>")
        assert result == pa.list_(pa.list_(pa.int32()))

    def test_map_string_string(self):
        result = presto_type_to_pyarrow("map<varchar,varchar>")
        assert result == pa.map_(pa.string(), pa.string())

    def test_map_string_bigint(self):
        result = presto_type_to_pyarrow("map<varchar,bigint>")
        assert result == pa.map_(pa.string(), pa.int64())

    def test_map_with_complex_value(self):
        result = presto_type_to_pyarrow("map<varchar,array<integer>>")
        assert result == pa.map_(pa.string(), pa.list_(pa.int32()))

    def test_array_of_map(self):
        result = presto_type_to_pyarrow("array<map<varchar,integer>>")
        assert result == pa.list_(pa.map_(pa.string(), pa.int32()))


class TestUnknownTypes:
    def test_unknown_defaults_to_string(self):
        assert presto_type_to_pyarrow("json") == pa.string()
        assert presto_type_to_pyarrow("ipaddress") == pa.string()
