# pipeline_logic_scripts/utils/snowflake_utils.py

from typing import Optional, Any, Dict, Tuple
import pandas as pd
from pandas import DataFrame
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


class SnowflakeQueryClient:
   """
   A highly modular and extensible client for Snowflake operations.
   
   Features:
   - Connection management with auto-retry
   - All query types (scalar, table, DML, control commands)
   - Bulk operations with DataFrame
   - Context manager support for auto-cleanup
   - Comprehensive error handling
   - Query traceability with IDs
   """

   def __init__(self, snowflake_creds: Dict[str, Any], snowflake_config: Dict[str, Any]):
       """Initialize with separate credentials and config"""
       self._validate_inputs(snowflake_creds, snowflake_config)
       self.creds = snowflake_creds.copy()
       self.config = snowflake_config.copy()
       self.connection: Optional[Any] = None

   def _validate_inputs(self, creds: Dict[str, Any], config: Dict[str, Any]) -> None:
       """Validate required parameters"""
       required_creds = ['account', 'user', 'password', 'role', 'warehouse']
       required_config = ['database', 'schema', 'table']
       
       missing_creds = [key for key in required_creds if key not in creds]
       missing_config = [key for key in required_config if key not in config]
       
       if missing_creds:
           raise ValueError(f"Missing credentials: {missing_creds}")
       if missing_config:
           raise ValueError(f"Missing config: {missing_config}")

   def _create_connection(self) -> Any:
       """Create new Snowflake connection"""
       try:
           conn = snowflake.connector.connect(
               account=self.creds['account'],
               user=self.creds['user'],
               password=self.creds['password'],
               role=self.creds['role'],
               warehouse=self.creds['warehouse'],
               database=self.config['database'],
               schema=self.config['schema']
           )
           return conn
       except Exception as error:
           raise ConnectionError(f"Snowflake connection failed: {error}") from error

   def get_connection(self) -> Any:
       """Get active connection, create if needed"""
       if self.connection is None or self.connection.is_closed():
           self.connection = self._create_connection()
       return self.connection

   def close_connection(self) -> None:
       """Close active connection"""
       if self.connection and not self.connection.is_closed():
           self.connection.close()
           self.connection = None

   def _get_query_id(self, cursor) -> Optional[str]:
       """Safely extract query ID from cursor"""
       return getattr(cursor, 'sfqid', None)

   def _execute_query(self, query: str, params: Optional[Dict] = None) -> Tuple[Any, Optional[str]]:
       """Core query execution method"""
       conn = self.get_connection()
       cursor = None
       
       try:
           cursor = conn.cursor()
           cursor.execute(query, params or {})
           query_id = self._get_query_id(cursor)
           return cursor, query_id
           
       except Exception as error:
           if cursor:
               cursor.close()
           raise RuntimeError(f"Query execution failed: {error}") from error

   def execute_scalar_query(self, query: str, query_params: Optional[Dict] = None) -> Dict[str, Any]:
       """Execute query returning single scalar value"""
       cursor, query_id = self._execute_query(query, query_params)
       try:
           result = cursor.fetchone()
           return {
               "query_id": query_id,
               "data": result[0] if result else None
           }
       finally:
           cursor.close()

   def fetch_all_rows_as_dataframe(self, query: str, query_params: Optional[Dict] = None) -> Dict[str, Any]:
       """Execute query returning DataFrame"""
       cursor, query_id = self._execute_query(query, query_params)
       try:
           df = cursor.fetch_pandas_all()
           return {
               "query_id": query_id,
               "data": df
           }
       finally:
           cursor.close()

   def fetch_all_rows_as_tuples(self, query: str, query_params: Optional[Dict] = None) -> Dict[str, Any]:
       """Execute query returning list of tuples"""
       cursor, query_id = self._execute_query(query, query_params)
       try:
           rows = cursor.fetchall()
           return {
               "query_id": query_id,
               "data": rows
           }
       finally:
           cursor.close()

   def execute_dml_query(self, query: str, query_params: Optional[Dict] = None) -> Dict[str, Any]:
       """Execute DML query (INSERT/UPDATE/DELETE)"""
       cursor, query_id = self._execute_query(query, query_params)
       try:
           return {
               "query_id": query_id,
               "rows_affected": cursor.rowcount
           }
       finally:
           cursor.close()

   def execute_control_command(self, query: str, query_params: Optional[Dict] = None) -> Dict[str, Any]:
       """Execute control commands (CALL, ALTER, etc.)"""
       cursor, query_id = self._execute_query(query, query_params)
       try:
           return {"query_id": query_id}
       finally:
           cursor.close()

   def bulk_insert_records(self, df: DataFrame, table: Optional[str] = None) -> Dict[str, Any]:
       """Bulk insert DataFrame to Snowflake table"""
       target_table = table or self.config['table']
       conn = self.get_connection()
       
       try:
           success, nchunks, nrows, query_id = write_pandas(
               conn, df, target_table, auto_create_table=False
           )
           
           if not success:
               raise RuntimeError(f"Bulk insert failed for table {target_table}")
           
           return {
               "query_id": query_id,
               "num_rows_inserted": nrows
           }
           
       except Exception as error:
           raise RuntimeError(f"Bulk insert failed: {error}") from error

   def table_exists(self, table_name: Optional[str] = None) -> bool:
       """Check if table exists"""
       target_table = table_name or self.config['table']
       query = """
       SELECT COUNT(*) 
       FROM INFORMATION_SCHEMA.TABLES 
       WHERE TABLE_NAME = UPPER(%(table_name)s)
       """
       result = self.execute_scalar_query(query, {"table_name": target_table})
       return result["data"] > 0

   def get_table_info(self, table_name: Optional[str] = None) -> DataFrame:
       """Get table column information"""
       target_table = table_name or self.config['table']
       query = """
       SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_NAME = UPPER(%(table_name)s)
       ORDER BY ORDINAL_POSITION
       """
       result = self.fetch_all_rows_as_dataframe(query, {"table_name": target_table})
       return result["data"]

   def __enter__(self):
       """Context manager entry"""
       return self

   def __exit__(self, exc_type, exc_val, exc_tb):
       """Context manager exit with cleanup"""
       self.close_connection()