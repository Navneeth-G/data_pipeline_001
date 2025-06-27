# pipeline_logic/core/record_status_updater.py
import pendulum
import pandas as pd
from typing import Dict, Any, Optional
from pipeline_logic.utils.log_generator import log

class RecordStatusUpdater:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def start_pipeline_execution(self, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Mark record as in_progress when pipeline starts"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET pipeline_status = 'in_progress',
            pipeline_start_time = %(start_time)s,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %(identifier_value)s
        """
        
        params = {
            'start_time': current_timestamp,
            'updated_time': current_timestamp,
            'identifier_value': identifier['value']
        }
        
        return self._execute_update(query, params, "Pipeline Start", identifier)
    
    def complete_stage(self, stage_name: str, actual_duration_minutes: float, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Mark a specific stage as completed"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        stage_number = self._get_stage_number(stage_name)
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET {stage_name}_status = 'completed',
            {stage_name}_end_time = %(end_time)s,
            {stage_name}_actual_duration_minutes = %(duration)s,
            completed_stage_number = %(stage_number)s,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %(identifier_value)s
        """
        
        params = {
            'end_time': current_timestamp,
            'duration': actual_duration_minutes,
            'stage_number': stage_number,
            'updated_time': current_timestamp,
            'identifier_value': identifier['value']
        }
        
        return self._execute_update(query, params, f"Stage Complete: {stage_name}", identifier)
    
    def start_stage(self, stage_name: str, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Mark a specific stage as started"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET {stage_name}_status = 'in_progress',
            {stage_name}_start_time = %(start_time)s,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %(identifier_value)s
        """
        
        params = {
            'start_time': current_timestamp,
            'updated_time': current_timestamp,
            'identifier_value': identifier['value']
        }
        
        return self._execute_update(query, params, f"Stage Start: {stage_name}", identifier)
    
    def complete_pipeline(self, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Mark entire pipeline as completed"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET pipeline_status = 'completed',
            pipeline_end_time = %(end_time)s,
            completed_stage_number = 4,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %(identifier_value)s
        """
        
        params = {
            'end_time': current_timestamp,
            'updated_time': current_timestamp,
            'identifier_value': identifier['value']
        }
        
        return self._execute_update(query, params, "Pipeline Complete", identifier)
    
    def fail_pipeline(self, error_details: str, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Mark pipeline as failed with error details"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET pipeline_status = 'failed',
            pipeline_end_time = %(end_time)s,
            pipeline_error_details = %(error_details)s,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %(identifier_value)s
        """
        
        params = {
            'end_time': current_timestamp,
            'error_details': error_details,
            'updated_time': current_timestamp,
            'identifier_value': identifier['value']
        }
        
        return self._execute_update(query, params, "Pipeline Failed", identifier)
    
    def _get_record_identifier(self, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, str]:
        """Get primary identifier for record (run_id or source_id fallback)"""
        if unique_run_id and not pd.isna(unique_run_id):
            return {"column": "unique_run_id", "value": str(unique_run_id)}
        elif unique_source_id and not pd.isna(unique_source_id):
            return {"column": "unique_source_id", "value": str(unique_source_id)}
        else:
            raise ValueError("Both unique_run_id and unique_source_id are missing")
    
    def _get_stage_number(self, stage_name: str) -> int:
        """Map stage name to stage number"""
        stage_mapping = {
            'source_discovery': 1,
            'source_to_stage_transfer': 2,
            'stage_to_target_transfer': 3,
            'audit': 4
        }
        return stage_mapping.get(stage_name, 0)
    
    def _execute_update(self, query: str, params: Dict[str, Any], operation: str, identifier: Dict[str, str]) -> Dict[str, Any]:
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
                    identifier_type=identifier['column'],
                    identifier_value=identifier['value'],
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
                    identifier_type=identifier['column'],
                    identifier_value=identifier['value']
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
                identifier_type=identifier['column'],
                identifier_value=identifier['value'],
                error_message=str(e)
            )
            
            return {
                "success": False,
                "error": str(e)
            }