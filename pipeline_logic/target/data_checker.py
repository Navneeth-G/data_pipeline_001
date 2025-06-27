# pipeline_logic/target/data_checker.py
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_logic.utils.time_utils import normalize_timestamp_to_iso8601
import pendulum

def is_target_data_already_processed(config: Dict[str, Any], source_query_window_start_time, source_query_window_end_time) -> bool:
    """Check if data already exists in Snowflake target table for the given time window"""
    
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
        
        # Query target table for records in time window
        target_table = f"{snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}"
        timestamp_field = config.get('target_timestamp_field', '@timestamp')
        
        check_query = f"""
        SELECT COUNT(*) as record_count
        FROM {target_table}
        WHERE {timestamp_field} >= %(start_time)s
          AND {timestamp_field} < %(end_time)s
        """
        
        params = {
            'start_time': start_time_iso,
            'end_time': end_time_iso
        }
        
        result = sf_client.execute_scalar_query(check_query, params)
        record_count = result.get('data', 0)
        
        data_exists = record_count > 0
        
        log.info(
            f"Target data check completed",
            log_key="Target Data Check",
            status="EXISTS" if data_exists else "NOT_EXISTS",
            target_table=target_table,
            record_count=record_count,
            time_window=f"{start_time_iso} to {end_time_iso}"
        )
        
        return data_exists
        
    except Exception as e:
        log.error(
            f"Error checking target data existence",
            log_key="Target Data Check",
            status="ERROR",
            error_message=str(e)
        )
        # Return False to allow processing to continue
        return False
    finally:
        if 'sf_client' in locals():
            sf_client.close_connection()


