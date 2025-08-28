#!/usr/bin/env python3
"""
Athena client for executing queries and returning polars DataFrames
"""

import boto3
import polars as pl
import time
from typing import Optional, Dict, Any
from botocore.config import Config


class AthenaClient:
    def __init__(
        self, 
        database: str, 
        output_location: str, 
        region: str = 'us-east-1',
        session: Optional[boto3.Session] = None,
        config: Optional[Config] = None,
        **client_kwargs
    ):
        # Use provided session or create new one
        if session:
            self.client = session.client('athena', config=config, **client_kwargs)
        else:
            # Default config with retries
            default_config = Config(
                region_name=region,
                retries={'max_attempts': 3, 'mode': 'adaptive'},
                max_pool_connections=50
            )
            final_config = config or default_config
            self.client = boto3.client('athena', config=final_config, **client_kwargs)
        
        self.database = database
        self.output_location = output_location
    
    def execute_query(
        self, 
        query: str, 
        wait_for_completion: bool = True,
        work_group: Optional[str] = None,
        query_context: Optional[Dict[str, str]] = None,
        result_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Execute a query and return the execution ID"""
        # Build query execution context
        context = {'Database': self.database}
        if query_context:
            context.update(query_context)
        
        # Build result configuration
        config = {'OutputLocation': self.output_location}
        if result_config:
            config.update(result_config)
        
        # Prepare execution parameters
        params = {
            'QueryString': query,
            'QueryExecutionContext': context,
            'ResultConfiguration': config
        }
        
        if work_group:
            params['WorkGroup'] = work_group
        
        try:
            response = self.client.start_query_execution(**params)
            execution_id = response['QueryExecutionId']
            
            if wait_for_completion:
                self._wait_for_completion(execution_id)
            
            return execution_id
            
        except Exception as e:
            raise Exception(f"Failed to execute query: {str(e)}")
    
    def _wait_for_completion(self, execution_id: str, max_wait_time: int = 300):
        """Wait for query to complete with exponential backoff"""
        start_time = time.time()
        sleep_time = 1
        
        while time.time() - start_time < max_wait_time:
            try:
                response = self.client.get_query_execution(QueryExecutionId=execution_id)
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    if status == 'FAILED':
                        reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                        error_msg = response['QueryExecution']['Status'].get('AthenaError', {}).get('ErrorMessage', '')
                        raise Exception(f"Query failed: {reason}. Error: {error_msg}")
                    elif status == 'CANCELLED':
                        raise Exception("Query was cancelled")
                    return status
                
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.5, 10)  # Exponential backoff, max 10s
                
            except Exception as e:
                # Only handle specific AWS exceptions, let our own exceptions through
                if "Query failed:" in str(e) or "Query was cancelled" in str(e):
                    raise  # Re-raise our own exceptions
                elif "InvalidRequestException" in str(type(e)) or "InvalidRequestException" in str(e):
                    raise Exception(f"Invalid query execution ID: {execution_id}")
                else:
                    # For any other exception, continue the loop or re-raise
                    break
        
        raise TimeoutError(f"Query did not complete within {max_wait_time} seconds")
    
    def get_results_polars(self, execution_id: str, max_rows: int = 1000) -> pl.DataFrame:
        """Get query results as a polars DataFrame with pagination support"""
        all_rows = []
        columns = None
        next_token = None
        
        try:
            while True:
                # Prepare pagination parameters
                params = {'QueryExecutionId': execution_id, 'MaxResults': min(max_rows, 1000)}
                if next_token:
                    params['NextToken'] = next_token
                
                response = self.client.get_query_results(**params)
                result_set = response['ResultSet']
                
                # Extract column names on first iteration
                if columns is None:
                    columns = [col['Name'] for col in result_set['ResultSetMetadata']['ColumnInfo']]
                    # Skip header row on first page
                    data_rows = result_set['Rows'][1:] if not next_token else result_set['Rows']
                else:
                    data_rows = result_set['Rows']
                
                # Extract data rows
                for row in data_rows:
                    row_data = []
                    for col in row['Data']:
                        # Handle different data types
                        value = col.get('VarCharValue', '')
                        if value == '':
                            # Check for null or other data types
                            if col.get('NullValue'):
                                value = None
                            else:
                                # Handle other potential data types
                                for key in ['BigIntValue', 'DoubleValue', 'BooleanValue']:
                                    if key in col:
                                        value = col[key]
                                        break
                        row_data.append(value)
                    all_rows.append(row_data)
                
                # Check for more results
                next_token = response.get('NextToken')
                if not next_token or len(all_rows) >= max_rows:
                    break
            
            # Truncate if we exceeded max_rows
            if len(all_rows) > max_rows:
                all_rows = all_rows[:max_rows]
            
            # Create polars DataFrame
            if not all_rows:
                return pl.DataFrame({col: [] for col in columns})
            
            data_dict = {col: [row[i] for row in all_rows] for i, col in enumerate(columns)}
            return pl.DataFrame(data_dict)
            
        except Exception as e:
            raise Exception(f"Failed to retrieve query results: {str(e)}")
    
    def get_query_info(self, execution_id: str) -> Dict[str, Any]:
        """Get detailed information about a query execution"""
        try:
            response = self.client.get_query_execution(QueryExecutionId=execution_id)
            return response['QueryExecution']
        except Exception as e:
            raise Exception(f"Failed to get query info: {str(e)}")
    
    def cancel_query(self, execution_id: str) -> bool:
        """Cancel a running query"""
        try:
            self.client.stop_query_execution(QueryExecutionId=execution_id)
            return True
        except Exception as e:
            raise Exception(f"Failed to cancel query: {str(e)}")
    
    def list_work_groups(self) -> list:
        """List available Athena work groups"""
        try:
            response = self.client.list_work_groups()
            return [wg['Name'] for wg in response['WorkGroups']]
        except Exception as e:
            raise Exception(f"Failed to list work groups: {str(e)}")
    
    @classmethod
    def from_session(cls, session: boto3.Session, database: str, output_location: str, **kwargs):
        """Create AthenaClient from existing boto3 session"""
        return cls(database, output_location, session=session, **kwargs)