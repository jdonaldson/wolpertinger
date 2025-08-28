# Claude Instructions for athena-polars

## Project Overview
This is a Python library for working with AWS Athena using Polars for data processing.

## Development Guidelines
- Use Polars for all data processing operations
- Follow Python packaging best practices with pyproject.toml
- Maintain compatibility with Python 3.8+

## Dependencies
- polars: Core data processing library
- boto3: AWS SDK for Athena integration  

## Development Dependencies
- pytest: Testing framework
- black: Code formatter
- ruff: Linter
- mypy: Type checker

## Commands
- Install dependencies: `pip install -e .[dev]`
- Run tests: `pytest`
- Format code: `black .`
- Lint code: `ruff check .`
- Type check: `mypy src/`