# pipeline_logic/core/stage_to_target_ingestion_manager.py
import pendulum
import pandas as pd
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.target.data_checker import is_target_data_already_processed
from pipeline_logic.target.target_cleaner import clean_target_before_ingestion  
from pipeline_logic.target.ingestion_engine import ingest_stage_to_target

class StageToTargetIngestionManager:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def execute_stage_to_target_ingestion(self, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Execute stage to target ingestion process with single database update"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        
        try:
            # Step 1: Get record info and validate status
            record_info = self._get_record_info(identifier)
            
            # Step 2: Check if already completed - exit immediately
            status_validation = self._validate_ingestion_status(record_info)
            if not status_validation["valid"]:
                return {"status": status_validation["reason"], "current_status": record_info.get('stage_to_target_transfer_status')}
            
            # Step 3: Track start time in memory
            start_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
            
            # Step 4: Check if target data already processed
            start_time_window = record_info['source_query_window_start_time']
            end_time_window = record_info['source_query_window_end_time']
            
            if is_target_data_already_processed(self.config, start_time_window, end_time_window):
                # Mark as completed without processing
                end_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
                actual_duration = (end_time - start_time).total_minutes()
                
                self._complete_ingestion_single_update(
                    identifier, start_time.to_iso8601_string(), 
                    end_time.to_iso8601_string(), actual_duration, 
                    result_message="Target data already processed - marked as completed"
                )
                
                return {
                    "status": "data_already_exists",
                    "marked_completed": True,
                    "actual_duration_minutes": actual_duration
                }
            
            # Step 5: Clean target before ingestion
            clean_target_before_ingestion(self.config, start_time_window, end_time_window)
            
            # Step 6: Run ingestion process
            ingestion_result = ingest_stage_to_target(self.config, start_time_window, end_time_window)
            
            # Step 7: Calculate actual duration
            end_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
            actual_duration = (end_time - start_time).total_minutes()
            
            # Step 8: SINGLE UPDATE with everything at once
            self._complete_ingestion_single_update(
                identifier, start_time.to_iso8601_string(), 
                end_time.to_iso8601_string(), actual_duration, 
                result_message=str(ingestion_result)
            )
            
            log.info(
                f"Stage to target ingestion completed successfully",
                log_key="Stage To Target Ingestion",
                status="SUCCESS",
                identifier_type=identifier['column'],
                identifier_value=identifier['value'],
                actual_duration_minutes=actual_duration
            )
            
            return {
                "status": "completed",
                "ingestion_result": ingestion_result,
                "actual_duration_minutes": actual_duration
            }
            
        except Exception as e:
            # Handle failure - reset to pending and increment retry
            self._handle_ingestion_failure(identifier, str(e))
            
            log.error(
                f"Stage to target ingestion failed",
                log_key="Stage To Target Ingestion",
                status="FAILED",
                identifier_type=identifier['column'],
                identifier_value=identifier['value'],
                error_message=str(e)
            )
            
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def _get_record_info(self, identifier: Dict[str, str]) -> Dict[str, Any]:
        """Get record information for ingestion"""
        query = f"""
        SELECT stage_to_target_transfer_status, source_query_window_start_time, 
               source_query_window_end_time, pipeline_retry_count
        FROM {self.config.get('drive_table')}
        WHERE {identifier['column']} = %({identifier['column']})s
        """
        
        params = {identifier['column']: identifier['value']}
        result = self.sf_client.fetch_all_rows_as_dataframe(query, params)
        df = result.get('data')
        
        if df.empty:
            raise ValueError(f"Record not found for {identifier['column']}: {identifier['value']}")
        
        return df.iloc[0].to_dict()
    
    def _validate_ingestion_status(self, record_info: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if ingestion can proceed"""
        status = record_info.get('stage_to_target_transfer_status')
        
        if status == 'completed':
            return {"valid": False, "reason": "already_completed"}
        elif status != 'pending':
            return {"valid": False, "reason": "not_pending"}
        else:
            return {"valid": True}
    
    def _complete_ingestion_single_update(self, identifier: Dict[str, str], start_time_iso: str, 
                                         end_time_iso: str, actual_duration: float, 
                                         result_message: str) -> None:
        """Complete ingestion with single database update"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET stage_to_target_transfer_status = 'completed',
            stage_to_target_transfer_start_time = %(start_time)s,
            stage_to_target_transfer_end_time = %(end_time)s,
            stage_to_target_transfer_actual_duration_minutes = %(actual_duration)s,
            stage_to_target_transfer_result = %(result_message)s,
            completed_stage_number = 3,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %({identifier['column']})s
        """
        
        params = {
            'start_time': start_time_iso,
            'end_time': end_time_iso,
            'actual_duration': actual_duration,
            'result_message': result_message,
            'updated_time': current_timestamp,
            identifier['column']: identifier['value']
        }
        
        result = self.sf_client.execute_dml_query(query, params)
        
        log.info(
            f"Stage to target single update completed successfully",
            log_key="Stage To Target Update",
            status="SUCCESS",
            rows_affected=result.get('rows_affected', 0),
            query_id=result.get('query_id')
        )
    
    def _handle_ingestion_failure(self, identifier: Dict[str, str], error_details: str) -> None:
        """Reset ingestion to pending and increment retry"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET stage_to_target_transfer_status = 'pending',
            stage_to_target_transfer_start_time = NULL,
            stage_to_target_transfer_end_time = NULL,
            stage_to_target_transfer_result = NULL,
            pipeline_retry_count = pipeline_retry_count + 1,
            pipeline_error_details = %(error_details)s,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %({identifier['column']})s
        """
        
        params = {
            'error_details': error_details,
            'updated_time': current_timestamp,
            identifier['column']: identifier['value']
        }
        
        self.sf_client.execute_dml_query(query, params)
    
    def _get_record_identifier(self, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, str]:
        """Get primary identifier for record"""
        if unique_run_id and not pd.isna(unique_run_id):
            return {"column": "unique_run_id", "value": str(unique_run_id)}
        elif unique_source_id and not pd.isna(unique_source_id):
            return {"column": "unique_source_id", "value": str(unique_source_id)}
        else:
            raise ValueError("Both unique_run_id and unique_source_id are missing")


