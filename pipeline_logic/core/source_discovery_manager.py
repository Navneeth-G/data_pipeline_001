# pipeline_logic/core/source_discovery_manager.py
import pendulum
import pandas as pd
import math
from typing import Dict, Any, Optional
from pipeline_logic.utils.log_generator import log
# Call external source_count function
# Assuming it returns an integer count
from external_module import source_count  # Replace with actual import


class SourceDiscoveryManager:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def execute_source_discovery(self, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Execute source discovery process with single database update"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        
        try:
            # Step 1: Validate source discovery status (1 read call - unavoidable)
            record_info = self._get_record_info(identifier)
            if not self._validate_source_discovery_status(record_info):
                return {"status": "skipped", "reason": "source_discovery not in pending status"}
            
            # Step 2: Track start time in memory
            start_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
            
            # Step 3: Get source count
            source_count = self._get_source_count(record_info)
            
            # Step 4: Calculate duration estimates
            duration_estimates = self._calculate_duration_estimates(source_count, identifier['value'])
            
            # Step 5: Calculate actual duration
            end_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
            actual_duration = (end_time - start_time).total_minutes()
            
            # Step 6: SINGLE UPDATE with everything at once
            self._complete_source_discovery_single_update(
                identifier, duration_estimates, source_count, 
                start_time.to_iso8601_string(), end_time.to_iso8601_string(), actual_duration
            )
            
            log.info(
                f"Source discovery completed successfully with single update",
                log_key="Source Discovery",
                status="SUCCESS",
                identifier_type=identifier['column'],
                identifier_value=identifier['value'],
                source_count=source_count,
                actual_duration_minutes=actual_duration
            )
            
            return {
                "status": "completed",
                "source_count": source_count,
                "duration_estimates": duration_estimates,
                "actual_duration_minutes": actual_duration
            }
            
        except Exception as e:
            # Handle failure - reset to pending and increment retry
            self._handle_source_discovery_failure(identifier, str(e))
            
            log.error(
                f"Source discovery failed",
                log_key="Source Discovery",
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
        """Get record information for source discovery"""
        query = f"""
        SELECT source_discovery_status, source_query_window_start_time, 
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
    
    def _validate_source_discovery_status(self, record_info: Dict[str, Any]) -> bool:
        """Check if source discovery status is pending"""
        return record_info.get('source_discovery_status') == 'pending'
    
    def _get_source_count(self, record_info: Dict[str, Any]) -> int:
        """Get source count from external function"""
        start_time = record_info['source_query_window_start_time']
        end_time = record_info['source_query_window_end_time']
        

        return source_count(self.config, start_time, end_time)
    
    def _complete_source_discovery_single_update(self, identifier: Dict[str, str], duration_estimates: Dict[str, int], 
                                                 source_count: int, start_time_iso: str, end_time_iso: str, 
                                                 actual_duration: float) -> None:
        """Complete source discovery with single database update"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET source_discovery_status = 'completed',
            source_discovery_start_time = %(start_time)s,
            source_discovery_end_time = %(end_time)s,
            source_discovery_actual_duration_minutes = %(actual_duration)s,
            completed_stage_number = 1,
            source_count = %(source_count)s,
            source_to_stage_transfer_expected_duration_minutes = %(source_to_stage_duration)s,
            stage_to_target_transfer_expected_duration_minutes = %(stage_to_target_duration)s,
            audit_expected_duration_minutes = %(audit_duration)s,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %({identifier['column']})s
        """
        
        params = {
            'start_time': start_time_iso,
            'end_time': end_time_iso,
            'actual_duration': actual_duration,
            'source_count': source_count,
            'source_to_stage_duration': duration_estimates['source_to_stage_transfer_expected_duration_minutes'],
            'stage_to_target_duration': duration_estimates['stage_to_target_transfer_expected_duration_minutes'],
            'audit_duration': duration_estimates['audit_expected_duration_minutes'],
            'updated_time': current_timestamp,
            identifier['column']: identifier['value']
        }
        
        result = self.sf_client.execute_dml_query(query, params)
        
        log.info(
            f"Single update completed successfully",
            log_key="Source Discovery Update",
            status="SUCCESS",
            rows_affected=result.get('rows_affected', 0),
            query_id=result.get('query_id')
        )
    
    def _handle_source_discovery_failure(self, identifier: Dict[str, str], error_details: str) -> None:
        """Reset source discovery to pending and increment retry"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET source_discovery_status = 'pending',
            source_discovery_start_time = NULL,
            source_discovery_end_time = NULL,
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
    
    def _calculate_duration_estimates(self, source_count: int, unique_source_id: str) -> Dict[str, int]:
        """Calculate expected durations with base time as minimum using proper scaling methods"""
        
        # Get configuration values
        avg_records_per_query_window = self.config.get('avg_records_per_query_window', 100000)
        skew_factor = self.config.get('skew_factor', 0.5)
        
        # Calculate record ratio
        if avg_records_per_query_window > 0:
            record_ratio = source_count / avg_records_per_query_window
        else:
            record_ratio = 1.0
        
        # Base processing times (these are MINIMUMS)
        base_source_to_stage_minutes = self.config.get('base_source_to_stage_minutes', 10)
        base_stage_to_target_minutes = self.config.get('base_stage_to_target_minutes', 5)
        base_audit_minutes = self.config.get('base_audit_minutes', 2)
        
        # Apply scaling functions using proper methods, but never go below base time
        if record_ratio <= 1.0:
            # Less data than expected - use base time
            source_to_stage_scaled = base_source_to_stage_minutes
            stage_to_target_scaled = base_stage_to_target_minutes  
            audit_scaled = base_audit_minutes
        else:
            # More data than expected - apply scaling using methods
            source_to_stage_scaled = self._linear_scaling(record_ratio, base_source_to_stage_minutes)
            stage_to_target_scaled = self._custom_power_scaling(record_ratio, base_stage_to_target_minutes, power=0.6)
            audit_scaled = self._square_root_scaling(record_ratio, base_audit_minutes)
        
        # Apply skew factor and ensure minimums
        duration_estimates = {
            "source_to_stage_transfer_expected_duration_minutes": max(
                int(source_to_stage_scaled * (1 + skew_factor)), 
                base_source_to_stage_minutes
            ),
            
            "stage_to_target_transfer_expected_duration_minutes": max(
                int(stage_to_target_scaled * (1 + skew_factor)), 
                base_stage_to_target_minutes
            ),
            
            "audit_expected_duration_minutes": max(
                int(audit_scaled * (1 + skew_factor)), 
                base_audit_minutes
            )
        }
        
        log.info(
            f"Duration estimates calculated using proper scaling methods",
            log_key="Duration Estimation",
            status="CALCULATED",
            source_count=source_count,
            record_ratio=f"{record_ratio:.2f}",
            used_base_minimum=record_ratio <= 1.0,
            scaling_methods={
                "source_to_stage": "linear",
                "stage_to_target": "power_0.6", 
                "audit": "square_root"
            },
            duration_estimates=duration_estimates
        )
        
        return duration_estimates

    def _linear_scaling(self, ratio: float, base_minutes: float) -> float:
        """Linear scaling: duration scales directly with data volume"""
        return ratio * base_minutes

    def _logarithmic_scaling(self, ratio: float, base_minutes: float) -> float:
        """Logarithmic scaling: diminishing returns as data volume increases"""
        if ratio <= 0:
            return base_minutes
        return base_minutes * (1 + math.log(1 + ratio))

    def _square_root_scaling(self, ratio: float, base_minutes: float) -> float:
        """Square root scaling: moderate sub-linear growth"""
        if ratio <= 0:
            return base_minutes
        return base_minutes * math.sqrt(ratio)

    def _custom_power_scaling(self, ratio: float, base_minutes: float, power: float = 0.7) -> float:
        """Custom power scaling: configurable sub-linear growth"""
        if ratio <= 0:
            return base_minutes
        return base_minutes * (ratio ** power)