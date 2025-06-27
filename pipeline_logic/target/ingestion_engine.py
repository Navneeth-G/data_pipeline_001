# pipeline_logic/target/ingestion_engine.py
import time
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.log_retry_decorators import retry
from pipeline_logic.utils.time_utils import normalize_timestamp_to_iso8601

class S3ToSnowflakeIngester:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
    
    def execute_ingestion(self, source_query_window_start_time, source_query_window_end_time, stage_to_target_transfer_expected_duration_minutes: float) -> Dict[str, Any]:
        """Execute S3 to Snowflake ingestion using Snowpipe tasks"""
        
        try:
            # Step 1: Execute Snowpipe task
            task_name = self.config.get('snowpipe_task_name')
            if not task_name:
                raise ValueError("snowpipe_task_name not found in config")
            
            self._execute_snowpipe_task(task_name, source_query_window_start_time)
            
            # Step 2: Calculate wait duration and wait
            wait_duration_seconds = max((stage_to_target_transfer_expected_duration_minutes * 60) - 5, 30)
            
            log.info(
                f"Waiting for Snowpipe ingestion to complete",
                log_key="Snowpipe Wait",
                status="WAITING",
                wait_duration_seconds=wait_duration_seconds,
                expected_duration_minutes=stage_to_target_transfer_expected_duration_minutes
            )
            
            time.sleep(wait_duration_seconds)
            
            log.info(
                f"S3 to Snowflake ingestion completed",
                log_key="S3 to Snowflake Ingestion",
                status="SUCCESS",
                task_name=task_name,
                wait_duration_seconds=wait_duration_seconds
            )
            
            return {
                "status": "success",
                "task_name": task_name,
                "wait_duration_seconds": wait_duration_seconds,
                "expected_duration_minutes": stage_to_target_transfer_expected_duration_minutes
            }
            
        except Exception as e:
            log.error(
                f"S3 to Snowflake ingestion failed",
                log_key="S3 to Snowflake Ingestion",
                status="FAILED",
                error_message=str(e)
            )
            raise
    
    @retry(max_attempts=3, delay_seconds=10)
    def _execute_snowpipe_task(self, task_name: str, source_query_window_start_time):
        """Execute Snowpipe task in Snowflake"""
        
        execute_task_query = f"EXECUTE TASK {task_name}"
        
        try:
            result = self.sf_client.execute_control_command(execute_task_query)
            
            log.info(
                f"Snowpipe task executed successfully",
                log_key="Snowpipe Task Execution",
                status="SUCCESS",
                task_name=task_name,
                query_id=result.get('query_id')
            )
            
        except Exception as e:
            log.error(
                f"Snowpipe task execution failed",
                log_key="Snowpipe Task Execution",
                status="FAILED",
                task_name=task_name,
                error_message=str(e)
            )
            raise Exception(f"Failed to execute Snowpipe task {task_name}: {str(e)}")

# Standalone function for import compatibility
def ingest_stage_to_target(config: Dict[str, Any], source_query_window_start_time, source_query_window_end_time) -> Dict[str, Any]:
    """Standalone function for stage to target ingestion"""
    # We need sf_client for Snowflake operations - this should be passed from the manager
    # For now, create a placeholder that raises an error if sf_client is not available
    from pipeline_logic.utils.snowflake_utils import SnowflakeQueryClient
    
    # Extract Snowflake credentials from config
    snowflake_creds = {
        'account': config.get('sf_account'),
        'user': config.get('sf_username'), 
        'password': config.get('sf_password'),
        'role': config.get('sf_role'),
        'warehouse': config.get('sf_warehouse')
    }
    
    snowflake_config = {
        'database': config.get('drive_database'),
        'schema': config.get('drive_schema'),
        'table': config.get('drive_table')
    }
    
    sf_client = SnowflakeQueryClient(snowflake_creds, snowflake_config)
    
    # Get expected duration from config (fallback if not in drive table)
    expected_duration = config.get('stage_to_target_transfer_expected_duration_minutes', 10)
    
    ingester = S3ToSnowflakeIngester(config, sf_client)
    return ingester.execute_ingestion(source_query_window_start_time, source_query_window_end_time, expected_duration)



