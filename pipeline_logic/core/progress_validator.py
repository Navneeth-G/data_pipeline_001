# pipeline_logic/core/progress_validator.py
import pendulum
from typing import Dict, Any, List
import pandas as pd
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.time_utils import normalize_timestamp_to_iso8601


class InProgressRecordsValidator:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def validate_all_in_progress_records(self) -> Dict[str, Any]:
        """Validate all in_progress records to identify stale/invalid ones"""
        start_time = pendulum.now()
        
        in_progress_records = self._fetch_in_progress_records()
        if in_progress_records.empty:
            log.info("No in_progress records found", log_key="Progress Validation", status="NO_RECORDS")
            return {"status": "no_in_progress_records"}
        
        results = {
            "valid_records": [],
            "invalid_records": [],
            "total_processed": len(in_progress_records)
        }
        
        for _, record in in_progress_records.iterrows():
            validation_result = self._validate_single_record(record.to_dict())
            
            if validation_result["is_valid_in_progress_record"]:
                results["valid_records"].append(validation_result)
            else:
                results["invalid_records"].append(validation_result)
        
        end_time = pendulum.now()
        results["loop_process_time_minutes"] = (end_time - start_time).total_minutes()
        
        log.info(
            f"Validated {len(in_progress_records)} in_progress records",
            log_key="Progress Validation",
            status="COMPLETED",
            valid_count=len(results["valid_records"]),
            invalid_count=len(results["invalid_records"])
        )
        
        return results

    def _fetch_in_progress_records(self) -> pd.DataFrame:
        """Fetch all records with pipeline_status = 'in_progress'"""
        query = f"""
        SELECT 
            unique_run_id, unique_source_id, pipeline_status,
            pipeline_start_time, completed_stage_number, pipeline_retry_count,
            source_discovery_status, source_discovery_expected_duration_minutes, 
            source_discovery_actual_duration_minutes,
            source_to_stage_transfer_status, source_to_stage_transfer_expected_duration_minutes,
            source_to_stage_transfer_actual_duration_minutes,
            stage_to_target_transfer_status, stage_to_target_transfer_expected_duration_minutes,
            stage_to_target_transfer_actual_duration_minutes,
            audit_status, audit_expected_duration_minutes, audit_actual_duration_minutes
        FROM {self.config.get('drive_table')}
        WHERE source_name = %(source_name)s
        AND source_category = %(source_category)s  
        AND source_subcategory = %(source_subcategory)s
        AND pipeline_status = 'in_progress'
        ORDER BY pipeline_priority ASC, source_query_window_start_time ASC
        """
        
        params = {
            'source_name': self.config.get('source_name'),
            'source_category': self.config.get('source_category'),
            'source_subcategory': self.config.get('source_subcategory')
        }
        
        result = self.sf_client.fetch_all_rows_as_dataframe(query, params)
        return result.get('data', pd.DataFrame())



    def _validate_single_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Check if single record exceeds acceptable runtime duration"""
        current_duration = self._calculate_current_runtime_minutes(record)
        acceptable_duration = self._calculate_acceptable_duration_minutes(record)
        
        is_valid = current_duration <= acceptable_duration
        
        return {
            "record_details": {
                "unique_run_id": record["unique_run_id"],
                "unique_source_id": record["unique_source_id"],
                "pipeline_status": record["pipeline_status"],
                "source_discovery_status": record["source_discovery_status"],
                "source_to_stage_transfer_status": record["source_to_stage_transfer_status"],
                "stage_to_target_transfer_status": record["stage_to_target_transfer_status"],
                "audit_status": record["audit_status"],
                "completed_stage_number": record["completed_stage_number"],
                "pipeline_retry_count": record["pipeline_retry_count"],
                "pipeline_start_time": record["pipeline_start_time"],
                "current_duration_minutes": current_duration,
                "acceptable_duration_minutes": acceptable_duration
            },
            "is_valid_in_progress_record": is_valid
        }
    


    def _calculate_current_runtime_minutes(self, record: Dict[str, Any]) -> float:
        """Calculate how long pipeline has been running"""
        current_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
        
        pipeline_start = record["pipeline_start_time"]
        if pd.isna(pipeline_start) or pipeline_start is None:
            return 0.0
            
        # Use your utility to handle different timestamp types
        start_time_iso = normalize_timestamp_to_iso8601(pipeline_start)
        start_time = pendulum.parse(start_time_iso)
        
        return (current_time - start_time).total_minutes()
    
    def _calculate_acceptable_duration_minutes(self, record: Dict[str, Any]) -> float:
        """Calculate maximum acceptable runtime based on stage progress"""
        pre_config_duration = self.config.get('pre_configuration_process_duration', 5)
        
        source_discovery_duration = self._get_stage_duration_minutes(
            record, "source_discovery", "source_discovery_status"
        )
        
        source_to_stage_duration = self._get_stage_duration_minutes(
            record, "source_to_stage_transfer", "source_to_stage_transfer_status"
        )
        
        stage_to_target_duration = self._get_stage_duration_minutes(
            record, "stage_to_target_transfer", "stage_to_target_transfer_status"
        )
        
        audit_duration = self._get_stage_duration_minutes(
            record, "audit", "audit_status"
        )
        
        total_duration = (pre_config_duration + source_discovery_duration + 
                         source_to_stage_duration + stage_to_target_duration + 
                         audit_duration)
        
        pipeline_avg = self.config.get('pipeline_specific_average_duration', float('inf'))
        return min(total_duration, pipeline_avg)
    
    def _get_stage_duration_minutes(self, record: Dict[str, Any], stage_prefix: str, status_col: str) -> float:
        """Get actual or expected duration for pipeline stage"""
        status = record[status_col]
        
        if status == "completed":
            actual_col = f"{stage_prefix}_actual_duration_minutes"
            return record.get(actual_col, 0) or 0
        else:
            expected_col = f"{stage_prefix}_expected_duration_minutes"
            return record.get(expected_col, 0) or 0




