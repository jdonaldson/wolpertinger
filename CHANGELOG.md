# Changelog

All notable changes to owlbear will be documented in this file.

## [0.2.1] - 2026-03-05

### Added

- PEP 561 `py.typed` marker for downstream type checking support
- Complete type annotations: all public methods now have explicit return types
- Pyright configuration in `pyproject.toml` (`typeCheckingMode = "standard"`)

### Changed

- Replaced `typing.Dict`, `typing.List`, `typing.Sequence` with builtin `dict`, `list` and `collections.abc.Sequence`

## [0.2.0] - 2026-03-05

### Breaking Changes

- Removed `OwlbearClient` alias — use `AthenaClient` directly
- Minimum Python version raised from 3.8 to 3.9

### Fixed

- `tinyint` now correctly maps to `int8` (was `int16`)
- `timestamp with time zone` preserves timezone as UTC instead of dropping it
- Nested complex types like `array<array<integer>>` now parse correctly via bracket-aware parsing
- `map<K,V>` types now parse actual key/value types (was hardcoded to `map<string,string>`)
- Empty strings in string/varchar columns are no longer silently converted to null
- All exceptions now use `raise ... from e` to preserve original tracebacks

### Added

- Parameterized queries: `AthenaClient.query(parameters=["value"])` and `TrinoClient.query(parameters=[value])`
- Athena result reuse: `AthenaClient.query(result_reuse_max_age=60)` enables query result caching
- Type mappings for `time`, `time with time zone`, `varbinary`, `binary`, `interval day to second`, `interval year to month`
- Published to PyPI: `pip install owlbear`

## [0.1.0] - 2024-08-28

### Added

- Initial release
- `AthenaClient` for executing Athena SQL and returning typed Polars DataFrames via PyArrow
- `TrinoClient` for direct Trino connections
- Shared `presto_type_to_pyarrow` type converter
- Automatic type mapping (integers, floats, decimals, timestamps, booleans, arrays, maps)
- Paginated result retrieval with configurable row limits
- Async query execution with exponential-backoff polling
- Work group support, query cancellation, and execution monitoring
- Optional extras: `[athena]`, `[trino]`, `[all]`
