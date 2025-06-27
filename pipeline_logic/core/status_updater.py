# pipeline_logic/core/record_status_updater.py
import pendulum
from typing import Dict, Any, Optional
from pipeline_logic.utils.log_generator import log

class RecordStatusUpdater:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def start_pipeline_execution(self, unique_source_id: str) -> Dict[str, Any]:
        """Mark record as in_progress when pipeline starts"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET pipeline_status = 'in_progress',
            pipeline_start_time = %(start_time)s,
            record_last_updated_time = %(updated_time)s
        WHERE unique_source_id = %(source_id)s
        """
        
        params = {
            'start_time': current_timestamp,
            'updated_time': current_timestamp,
            'source_id': unique_source_id
        }
        
        return self._execute_update(query, params, "Pipeline Start", unique_source_id)
    
    def complete_stage(self, unique_source_id: str, stage_name: str, actual_duration_minutes: float) -> Dict[str, Any]:
        """Mark a specific stage as completed"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        stage_number = self._get_stage_number(stage_name)
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET {stage_name}_status = 'completed',
            {stage_name}_end_time = %(end_time)s,
            {stage_name}_actual_duration_minutes = %(duration)s,
            current_stage_number = %(stage_number)s,
            record_last_updated_time = %(updated_time)s
        WHERE unique_source_id = %(source_id)s
        """
        
        params = {
            'end_time': current_timestamp,
            'duration': actual_duration_minutes,
            'stage_number': stage_number,
            'updated_time': current_timestamp,
            'source_id': unique_source_id
        }
        
        return self._execute_update(query, params, f"Stage Complete: {stage_name}", unique_source_id)
    
    def start_stage(self, unique_source_id: str, stage_name: str) -> Dict[str, Any]:
        """Mark a specific stage as started"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET {stage_name}_status = 'in_progress',
            {stage_name}_start_time = %(start_time)s,
            record_last_updated_time = %(updated_time)s
        WHERE unique_source_id = %(source_id)s
        """
        
        params = {
            'start_time': current_timestamp,
            'updated_time': current_timestamp,
            'source_id': unique_source_id
        }
        
        return self._execute_update(query, params, f"Stage Start: {stage_name}", unique_source_id)
    
    def complete_pipeline(self, unique_source_id: str) -> Dict[str, Any]:
        """Mark entire pipeline as completed"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET pipeline_status = 'completed',
            pipeline_end_time = %(end_time)s,
            current_stage_number = 4,
            record_last_updated_time = %(updated_time)s
        WHERE unique_source_id = %(source_id)s
        """
        
        params = {
            'end_time': current_timestamp,
            'updated_time': current_timestamp,
            'source_id': unique_source_id
        }
        
        return self._execute_update(query, params, "Pipeline Complete", unique_source_id)
    
    def fail_pipeline(self, unique_source_id: str, error_details: str) -> Dict[str, Any]:
        """Mark pipeline as failed with error details"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET pipeline_status = 'failed',
            pipeline_end_time = %(end_time)s,
            pipeline_error_details = %(error_details)s,
            record_last_updated_time = %(updated_time)s
        WHERE unique_source_id = %(source_id)s
        """
        
        params = {
            'end_time': current_timestamp,
            'error_details': error_details,
            'updated_time': current_timestamp,
            'source_id': unique_source_id
        }
        
        return self._execute_update(query, params, "Pipeline Failed", unique_source_id)
    
    def _get_stage_number(self, stage_name: str) -> int:
        """Map stage name to stage number"""
        stage_mapping = {
            'source_discovery': 1,
            'source_to_stage_transfer': 2,
            'stage_to_target_transfer': 3,
            'audit': 4
        }
        return stage_mapping.get(stage_name, 0)
    
    def _execute_update(self, query: str, params: Dict[str, Any], operation: str, source_id: str) -> Dict[str, Any]:
        """Execute update query with logging"""
        try:
            result = self.sf_client.execute_dml_query(query, params)
            rows_affected = result.get('rows_affected', 0)
            
            if rows_affected > 0:
                log.info(
                    f"{operation} successful",
                    log_key="Record Status Update",
                    status="SUCCESS",
                    operation=operation,
                    source_id=source_id,
                    rows_affected=rows_affected
                )
                
                return {
                    "success": True,
                    "rows_affected": rows_affected,
                    "query_id": result.get('query_id')
                }
            else:
                log.warning(
                    f"{operation} - no rows affected",
                    log_key="Record Status Update",
                    status="NO_ROWS",
                    operation=operation,
                    source_id=source_id
                )
                
                return {
                    "success": False,
                    "error": "No rows affected"
                }
                
        except Exception as e:
            log.error(
                f"{operation} failed",
                log_key="Record Status Update",
                status="FAILED",
                operation=operation,
                source_id=source_id,
                error_message=str(e)
            )
            
            return {
                "success": False,
                "error": str(e)
            }



