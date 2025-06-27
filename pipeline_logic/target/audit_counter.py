# pipeline_logic/target/audit_counter.py
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_logic.utils.time_utils import normalize_timestamp_to_iso8601

def get_target_count_for_audit(config: Dict[str, Any], source_query_window_start_time, source_query_window_end_time) -> int:
    """Get target record count from Snowflake for audit verification"""
    
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
        
        # Query target table for record count in time window
        target_table = f"{snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}"
        timestamp_field = config.get('target_timestamp_field', '@timestamp')
        
        count_query = f"""
        SELECT COUNT(*) as record_count
        FROM {target_table}
        WHERE {timestamp_field} >= %(start_time)s
          AND {timestamp_field} < %(end_time)s
        """
        
        params = {
            'start_time': start_time_iso,
            'end_time': end_time_iso
        }
        
        result = sf_client.execute_scalar_query(count_query, params)
        target_count = result.get('data', 0)
        
        log.info(
            f"Retrieved target count for audit",
            log_key="Target Count Audit",
            status="SUCCESS",
            target_table=target_table,
            target_count=target_count,
            time_window=f"{start_time_iso} to {end_time_iso}"
        )
        
        return target_count
        
    except Exception as e:
        log.error(
            f"Error getting target count for audit",
            log_key="Target Count Audit",
            status="ERROR",
            error_message=str(e)
        )
        # Return 0 on error to trigger count mismatch handling
        return 0
    finally:
        if 'sf_client' in locals():
            sf_client.close_connection()



