"""Pytest configuration and fixtures"""

import pytest
import polars as pl
from typing import Dict, Any


@pytest.fixture
def sample_athena_response():
    """Sample Athena query response for testing"""
    return {
        'ResultSet': {
            'ResultSetMetadata': {
                'ColumnInfo': [
                    {'Name': 'id', 'Type': 'integer'},
                    {'Name': 'name', 'Type': 'varchar'},
                    {'Name': 'score', 'Type': 'double'},
                    {'Name': 'active', 'Type': 'boolean'}
                ]
            },
            'Rows': [
                # Header row
                {
                    'Data': [
                        {'VarCharValue': 'id'}, 
                        {'VarCharValue': 'name'}, 
                        {'VarCharValue': 'score'},
                        {'VarCharValue': 'active'}
                    ]
                },
                # Data rows
                {
                    'Data': [
                        {'VarCharValue': '1'}, 
                        {'VarCharValue': 'Alice'}, 
                        {'VarCharValue': '95.5'},
                        {'VarCharValue': 'true'}
                    ]
                },
                {
                    'Data': [
                        {'VarCharValue': '2'}, 
                        {'VarCharValue': 'Bob'}, 
                        {'VarCharValue': '87.2'},
                        {'VarCharValue': 'false'}
                    ]
                },
                {
                    'Data': [
                        {'VarCharValue': '3'}, 
                        {'NullValue': True}, 
                        {'VarCharValue': '92.8'},
                        {'VarCharValue': 'true'}
                    ]
                }
            ]
        }
    }


@pytest.fixture
def expected_dataframe():
    """Expected polars DataFrame from sample response"""
    return pl.DataFrame({
        'id': ['1', '2', '3'],
        'name': ['Alice', 'Bob', None],
        'score': ['95.5', '87.2', '92.8'],
        'active': ['true', 'false', 'true']
    })


@pytest.fixture
def query_execution_response():
    """Sample query execution response"""
    return {
        'QueryExecution': {
            'QueryExecutionId': 'test-execution-id',
            'Query': 'SELECT * FROM test_table',
            'QueryExecutionContext': {'Database': 'test_db'},
            'ResultConfiguration': {
                'OutputLocation': 's3://test-bucket/results/test-execution-id.csv'
            },
            'Status': {
                'State': 'SUCCEEDED',
                'SubmissionDateTime': '2024-01-01T00:00:00Z',
                'CompletionDateTime': '2024-01-01T00:01:00Z'
            },
            'Statistics': {
                'EngineExecutionTimeInMillis': 1000,
                'DataProcessedInBytes': 1024,
                'TotalExecutionTimeInMillis': 2000
            }
        }
    }