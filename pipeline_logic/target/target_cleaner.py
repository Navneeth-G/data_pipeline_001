# pipeline_logic/target/target_cleaner.py
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_logic.utils.time_utils import normalize_timestamp_to_iso8601

def clean_target_before_ingestion(config: Dict[str, Any], source_query_window_start_time, source_query_window_end_time) -> None:
    """Clean/delete existing target data before new ingestion"""
    
    try:
        # Create Snowflake client
        snowflake_creds = {
            'account': config.get('sf_account'),
            'user': config.get('sf_username'), 
            'password': config.get('sf_password'),
            'role': config.get('sf_role'),
            'warehouse': config.get('sf_warehouse')
        }
        
        snowflake_config = {
            'database': config.get('target_database'),
            'schema': config.get('target_schema'),
            'table': config.get('target_table')
        }
        
        sf_client = SnowflakeQueryClient(snowflake_creds, snowflake_config)
        
        # Convert timestamps to consistent format
        start_time_iso = normalize_timestamp_to_iso8601(source_query_window_start_time)
        end_time_iso = normalize_timestamp_to_iso8601(source_query_window_end_time)
        
        # Delete records from target table in time window
        target_table = f"{snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}"
        timestamp_field = config.get('target_timestamp_field', '@timestamp')
        
        delete_query = f"""
        DELETE FROM {target_table}
        WHERE {timestamp_field} >= %(start_time)s
          AND {timestamp_field} < %(end_time)s
        """
        
        params = {
            'start_time': start_time_iso,
            'end_time': end_time_iso
        }
        
        result = sf_client.execute_dml_query(delete_query, params)
        rows_deleted = result.get('rows_affected', 0)
        
        log.info(
            f"Target data cleaning completed",
            log_key="Target Data Cleaning",
            status="SUCCESS",
            target_table=target_table,
            rows_deleted=rows_deleted,
            time_window=f"{start_time_iso} to {end_time_iso}"
        )
        
    except Exception as e:
        log.error(
            f"Error during target data cleaning",
            log_key="Target Data Cleaning",
            status="ERROR",
            error_message=str(e)
        )
        # Don't raise exception - cleaning failure shouldn't stop pipeline
    finally:
        if 'sf_client' in locals():
            sf_client.close_connection()