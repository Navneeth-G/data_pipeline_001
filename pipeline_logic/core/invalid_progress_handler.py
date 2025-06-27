# pipeline_logic/core/invalid_progress_handler.py
import pendulum
from typing import Dict, Any, List
import pandas as pd
from pipeline_logic.utils.log_generator import log

class InvalidProgressHandler:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def reset_invalid_records(self, invalid_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Reset all invalid in_progress records to pending state"""
        if not invalid_records:
            log.info("No invalid records to reset", log_key="Invalid Progress Reset", status="NO_RECORDS")
            return {"status": "no_invalid_records", "reset_count": 0}
        
        reset_results = {
            "successful_resets": [],
            "failed_resets": [],
            "total_processed": len(invalid_records)
        }
        
        for invalid_record in invalid_records:
            record_details = invalid_record["record_details"]
            reset_result = self._reset_single_record(record_details)
            
            if reset_result["success"]:
                reset_results["successful_resets"].append(reset_result)
            else:
                reset_results["failed_resets"].append(reset_result)
        
        log.info(
            f"Reset operation completed",
            log_key="Invalid Progress Reset",
            status="COMPLETED",
            total_processed=len(invalid_records),
            successful_count=len(reset_results["successful_resets"]),
            failed_count=len(reset_results["failed_resets"])
        )
        
        return reset_results
    
    def _reset_single_record(self, record_details: Dict[str, Any]) -> Dict[str, Any]:
        """Reset a single invalid record"""
        identifier = self._get_record_identifier(record_details)
        
        try:
            # Build update query
            update_query, params = self._build_reset_query(record_details, identifier)
            
            # Execute update
            result = self.sf_client.execute_dml_query(update_query, params)
            rows_affected = result.get('rows_affected', 0)
            
            if rows_affected > 0:
                log.info(
                    f"Successfully reset record",
                    log_key="Single Record Reset",
                    status="SUCCESS",
                    identifier=identifier["value"],
                    identifier_type=identifier["type"],
                    rows_affected=rows_affected
                )
                
                return {
                    "success": True,
                    "identifier": identifier,
                    "rows_affected": rows_affected,
                    "query_id": result.get('query_id')
                }
            else:
                return {
                    "success": False,
                    "identifier": identifier,
                    "error": "No rows affected"
                }
                
        except Exception as e:
            log.error(
                f"Failed to reset record",
                log_key="Single Record Reset",
                status="FAILED",
                identifier=identifier["value"],
                error_message=str(e)
            )
            
            return {
                "success": False,
                "identifier": identifier,
                "error": str(e)
            }
    
    def _get_record_identifier(self, record_details: Dict[str, Any]) -> Dict[str, str]:
        """Get primary identifier for record (run_id or source_id)"""
        unique_run_id = record_details.get("unique_run_id")
        unique_source_id = record_details.get("unique_source_id")
        
        if unique_run_id and not pd.isna(unique_run_id):
            return {"type": "unique_run_id", "value": str(unique_run_id)}
        elif unique_source_id and not pd.isna(unique_source_id):
            return {"type": "unique_source_id", "value": str(unique_source_id)}
        else:
            raise ValueError("Both unique_run_id and unique_source_id are missing or null")
    
    def _build_reset_query(self, record_details: Dict[str, Any], identifier: Dict[str, str]) -> tuple:
        """Build SQL update query to reset record"""
        # Get current stage statuses to determine what to reset
        stage_resets = self._determine_stage_resets(record_details)
        
        # Base pipeline reset
        set_clauses = [
            "pipeline_status = 'pending'",
            "pipeline_start_time = NULL",
            "pipeline_end_time = NULL",
            "pipeline_retry_count = pipeline_retry_count + 1",
            "record_last_updated_time = CURRENT_TIMESTAMP()"
        ]
        
        # Add stage-specific resets
        set_clauses.extend(stage_resets)
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET {', '.join(set_clauses)}
        WHERE {identifier['type']} = %({identifier['type']})s
        """
        
        params = {identifier['type']: identifier['value']}
        
        return query, params
    
    def _determine_stage_resets(self, record_details: Dict[str, Any]) -> List[str]:
        """Determine which stages need to be reset (skip completed ones)"""
        stage_configs = [
            {
                "prefix": "source_discovery",
                "status_col": "source_discovery_status"
            },
            {
                "prefix": "source_to_stage_transfer", 
                "status_col": "source_to_stage_transfer_status"
            },
            {
                "prefix": "stage_to_target_transfer",
                "status_col": "stage_to_target_transfer_status"
            },
            {
                "prefix": "audit",
                "status_col": "audit_status"
            }
        ]
        
        reset_clauses = []
        
        for stage in stage_configs:
            current_status = record_details.get(stage["status_col"])
            
            # Only reset if not completed
            if current_status != "completed":
                reset_clauses.extend([
                    f"{stage['status_col']} = 'pending'",
                    f"{stage['prefix']}_start_time = NULL",
                    f"{stage['prefix']}_end_time = NULL"
                ])
        
        return reset_clauses



