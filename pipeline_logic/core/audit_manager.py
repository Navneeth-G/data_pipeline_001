# pipeline_logic/core/audit_manager.py
import time
import pendulum
import pandas as pd
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.target.audit_counter import get_target_count_for_audit
from pipeline_logic.stage.stage_cleaner import clean_stage_before_ingestion
from pipeline_logic.target.target_cleaner import clean_target_before_ingestion

class AuditManager:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def execute_audit_process(self, unique_run_id: str = None, unique_source_id: str = None) -> Dict[str, Any]:
        """Execute audit process with optional crazy retry logic for Snowpipe"""
        identifier = self._get_record_identifier(unique_run_id, unique_source_id)
        
        try:
            # Step 1: Get record info and validate status
            record_info = self._get_record_info(identifier)
            
            # Step 2: Check if already completed - exit immediately
            status_validation = self._validate_audit_status(record_info)
            if not status_validation["valid"]:
                return {"status": status_validation["reason"], "current_status": record_info.get('audit_status')}
            
            # Step 3: Track start time in memory
            start_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
            
            # Step 4: Get source count from drive table (stored during source discovery)
            source_count = record_info.get('source_count')
            if not source_count or source_count == 0:
                raise ValueError("Source count not found in drive table - source discovery may not have completed")
            
            # Step 5: Get target count using modular function
            start_time_window = record_info['source_query_window_start_time']
            end_time_window = record_info['source_query_window_end_time']
            target_count = get_target_count_for_audit(self.config, start_time_window, end_time_window)
            
            # Step 6: Calculate count differences and percentages
            count_difference = target_count - source_count
            count_difference_percentage = self._calculate_difference_percentage(source_count, target_count)
            accepted_tolerance = self.config.get('accepted_tolerance_percentage', 1.0)
            
            # Step 7: Check if counts match within tolerance
            if abs(count_difference_percentage) <= accepted_tolerance:
                # Counts match immediately - complete successfully
                end_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
                actual_duration = (end_time - start_time).total_minutes()
                
                self._complete_audit_success(
                    identifier, start_time.to_iso8601_string(), end_time.to_iso8601_string(),
                    actual_duration, source_count, target_count, count_difference, 
                    count_difference_percentage, "IMMEDIATE_MATCH"
                )
                
                return {
                    "status": "completed",
                    "count_match_status": "matched",
                    "source_count": source_count,
                    "target_count": target_count,
                    "difference_percentage": count_difference_percentage,
                    "retry_attempts_used": 0
                }
            
            # Step 8: Handle count mismatch
            # Check if crazy retry logic is enabled (for Snowpipe or similar async systems)
            enable_crazy_retry = self.config.get('enable_crazy_retry_logic', False)
            
            if enable_crazy_retry:
                # Use crazy adaptive retry logic for async systems like Snowpipe
                return self._execute_crazy_retry_logic(
                    identifier, source_count, target_count, accepted_tolerance, 
                    start_time_window, end_time_window, start_time
                )
            else:
                # Standard approach - direct cleanup and reset
                return self._perform_standard_mismatch_handling(
                    identifier, start_time_window, end_time_window, source_count, target_count,
                    count_difference, count_difference_percentage
                )
            
        except Exception as e:
            # Handle failure - reset to pending and increment retry
            self._handle_audit_failure(identifier, str(e))
            
            log.error(
                f"Audit process failed",
                log_key="Audit Process",
                status="FAILED",
                identifier_type=identifier['column'],
                identifier_value=identifier['value'],
                error_message=str(e)
            )
            
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def _perform_standard_mismatch_handling(self, identifier: Dict[str, str], start_time_window, end_time_window,
                                          source_count: int, target_count: int, count_difference: int, 
                                          count_difference_percentage: float) -> Dict[str, Any]:
        """Standard mismatch handling - direct cleanup and reset"""
        
        log.info(
            f"Standard mismatch handling - direct cleanup and reset",
            log_key="Audit Process",
            status="STANDARD_MISMATCH_HANDLING",
            identifier_value=identifier['value'],
            source_count=source_count,
            target_count=target_count,
            difference_percentage=f"{count_difference_percentage:.2f}%"
        )
        
        return self._perform_cleaning_and_reset(
            identifier, start_time_window, end_time_window, source_count, target_count,
            count_difference, count_difference_percentage, "not_matched"
        )
    
    def _execute_crazy_retry_logic(self, identifier: Dict[str, str], source_count: int, 
                                  initial_target_count: int, tolerance_percentage: float,
                                  start_time_window, end_time_window, audit_start_time) -> Dict[str, Any]:
        """Execute crazy adaptive retry logic for async systems like Snowpipe"""
        
        # Calculate base retry attempts based on initial difference
        initial_difference_percentage = self._calculate_difference_percentage(source_count, initial_target_count)
        base_retry_attempts = self._calculate_base_retry_attempts(abs(initial_difference_percentage))
        
        log.info(
            f"PHASE 1: Starting crazy adaptive audit logic",
            log_key="Crazy Audit Logic",
            status="PHASE_1_STARTED", 
            identifier_value=identifier['value'],
            source_count=source_count,
            initial_target_count=initial_target_count,
            initial_difference_percentage=f"{initial_difference_percentage:.2f}%",
            base_retry_attempts=base_retry_attempts
        )
        
        # First retry after 60 seconds to check for improvement
        log.info(
            f"PHASE 1: Initial wait before first retry",
            log_key="Crazy Audit Logic",
            status="INITIAL_WAIT",
            identifier_value=identifier['value']
        )
        
        time.sleep(60)  # Wait 1 minute for async processing
        
        # First recount - only target count (source won't change)
        first_retry_count = get_target_count_for_audit(self.config, start_time_window, end_time_window)
        initial_improvement = first_retry_count - initial_target_count
        count_difference = first_retry_count - source_count
        count_difference_percentage = self._calculate_difference_percentage(source_count, first_retry_count)
        
        log.info(
            f"PHASE 1: First retry recount completed",
            log_key="Crazy Audit Logic",
            status="FIRST_RETRY_RECOUNT",
            identifier_value=identifier['value'],
            initial_target_count=initial_target_count,
            first_retry_count=first_retry_count,
            initial_improvement=initial_improvement,
            count_difference=count_difference,
            count_difference_percentage=f"{count_difference_percentage:.2f}%"
        )
        
        # Check if counts now match after first retry
        if abs(count_difference_percentage) <= tolerance_percentage:
            end_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
            actual_duration = (end_time - audit_start_time).total_minutes()
            
            self._complete_audit_success(
                identifier, audit_start_time.to_iso8601_string(), end_time.to_iso8601_string(),
                actual_duration, source_count, first_retry_count, count_difference, 
                count_difference_percentage, "MATCH_AFTER_FIRST_RETRY"
            )
            
            return {
                "status": "completed",
                "count_match_status": "matched", 
                "source_count": source_count,
                "target_count": first_retry_count,
                "difference_percentage": count_difference_percentage,
                "retry_attempts_used": 1
            }
        
        # Check for improvement - if improving, continue with adaptive logic
        if initial_improvement > 0:
            return self._execute_crazy_adaptive_logic(
                identifier, source_count, first_retry_count, tolerance_percentage,
                base_retry_attempts, initial_improvement, start_time_window, end_time_window, audit_start_time
            )
        else:
            # No improvement - go straight to cleanup and pipeline reset
            log.info(
                f"No improvement detected - proceeding to cleanup and reset",
                log_key="Crazy Audit Logic",
                status="NO_IMPROVEMENT_CLEANUP",
                identifier_value=identifier['value']
            )
            
            return self._perform_cleaning_and_reset(
                identifier, start_time_window, end_time_window, source_count, first_retry_count,
                count_difference, count_difference_percentage, "not_matched"
            )
    
    def _calculate_base_retry_attempts(self, difference_percentage: float) -> int:
        """Calculate base retry attempts based on initial count difference"""
        
        if difference_percentage <= 5:
            base_attempts = 2  # Small difference - quick retries
        elif difference_percentage <= 15:
            base_attempts = 3  # Medium difference 
        elif difference_percentage <= 30:
            base_attempts = 4  # Large difference
        elif difference_percentage <= 50:
            base_attempts = 5  # Very large difference
        else:
            base_attempts = 6  # Massive difference - more patience needed
        
        log.info(
            f"Calculated base retry attempts",
            log_key="Crazy Audit Logic",
            status="BASE_ATTEMPTS_CALCULATED",
            difference_percentage=f"{difference_percentage:.2f}%",
            base_attempts=base_attempts
        )
        
        return base_attempts
    
    def _execute_crazy_adaptive_logic(self, identifier: Dict[str, str], source_count: int, current_count: int, 
                                    tolerance_percentage: float, base_retry_attempts: int, 
                                    initial_improvement: int, start_time_window, end_time_window, 
                                    audit_start_time) -> Dict[str, Any]:
        """Execute crazy adaptive logic when improvement is detected"""
        
        # CRAZY LOGIC: Scale additional retries based on improvement rate
        improvement_rate = initial_improvement / 60  # Records per second improvement rate
        
        # Calculate additional retries based on improvement rate
        if improvement_rate >= 100:  # Very fast improvement
            additional_retries = base_retry_attempts * 2
            wait_multiplier = 1.5
        elif improvement_rate >= 50:   # Fast improvement 
            additional_retries = base_retry_attempts * 1.5
            wait_multiplier = 1.3
        elif improvement_rate >= 10:   # Moderate improvement
            additional_retries = base_retry_attempts * 1.2
            wait_multiplier = 1.2
        else:  # Slow but steady improvement
            additional_retries = base_retry_attempts
            wait_multiplier = 1.0
        
        max_total_retries = int(base_retry_attempts + additional_retries)
        
        log.info(
            f"PHASE 2: Crazy adaptive logic activated",
            log_key="Crazy Audit Logic",
            status="PHASE_2_ACTIVATED",
            identifier_value=identifier['value'],
            initial_improvement=initial_improvement,
            improvement_rate=f"{improvement_rate:.2f} records/sec",
            base_retry_attempts=base_retry_attempts,
            additional_retries=int(additional_retries),
            max_total_retries=max_total_retries,
            wait_multiplier=wait_multiplier
        )
        
        previous_count = current_count
        consecutive_no_improvement = 0
        
        # Adaptive retry loop (starting from retry 2 since we already did 1)
        for retry_num in range(2, max_total_retries + 1):
            
            # Dynamic wait time calculation
            base_wait = 60  # 1 minute base
            dynamic_wait = int(base_wait * wait_multiplier * (1 + (retry_num - 2) * 0.1))  # Gradually increase
            dynamic_wait = min(dynamic_wait, 180)  # Cap at 3 minutes max
            
            log.info(
                f"PHASE 2: Retry {retry_num}/{max_total_retries} - Dynamic wait",
                log_key="Crazy Audit Logic",
                status="PHASE_2_WAIT",
                identifier_value=identifier['value'],
                retry_num=retry_num,
                wait_duration_seconds=dynamic_wait,
                consecutive_no_improvement=consecutive_no_improvement
            )
            
            time.sleep(dynamic_wait)
            
            # Recount target only (source count doesn't change)
            new_count = get_target_count_for_audit(self.config, start_time_window, end_time_window)
            count_improvement = new_count - previous_count
            count_difference = new_count - source_count
            count_difference_percentage = self._calculate_difference_percentage(source_count, new_count)
            
            log.info(
                f"PHASE 2: Retry {retry_num} recount completed",
                log_key="Crazy Audit Logic",
                status="PHASE_2_RECOUNT",
                identifier_value=identifier['value'],
                retry_num=retry_num,
                previous_count=previous_count,
                new_count=new_count,
                count_improvement=count_improvement,
                count_difference=count_difference,
                count_difference_percentage=f"{count_difference_percentage:.2f}%"
            )
            
            # Check if counts now match within tolerance
            if abs(count_difference_percentage) <= tolerance_percentage:
                end_time = pendulum.now(self.config.get('timezone', 'America/Los_Angeles'))
                actual_duration = (end_time - audit_start_time).total_minutes()
                
                self._complete_audit_success(
                    identifier, audit_start_time.to_iso8601_string(), end_time.to_iso8601_string(),
                    actual_duration, source_count, new_count, count_difference, 
                    count_difference_percentage, f"MATCH_AFTER_CRAZY_RETRY_{retry_num}"
                )
                
                return {
                    "status": "completed",
                    "count_match_status": "matched",
                    "source_count": source_count,
                    "target_count": new_count,
                    "difference_percentage": count_difference_percentage,
                    "retry_attempts_used": retry_num
                }
            
            # Check for continued improvement
            if count_improvement > 0:
                consecutive_no_improvement = 0
                log.info(
                    f"Continued improvement detected!",
                    log_key="Crazy Audit Logic",
                    status="CONTINUED_IMPROVEMENT",
                    identifier_value=identifier['value'],
                    retry_num=retry_num,
                    count_improvement=count_improvement
                )
            else:
                consecutive_no_improvement += 1
                log.info(
                    f"No improvement in retry {retry_num}",
                    log_key="Crazy Audit Logic",
                    status="NO_IMPROVEMENT",
                    identifier_value=identifier['value'],
                    retry_num=retry_num,
                    consecutive_no_improvement=consecutive_no_improvement
                )
                
                # CRAZY LOGIC: Stop if no improvement for 2 consecutive retries
                if consecutive_no_improvement >= 2:
                    log.info(
                        f"Stopping crazy logic - no improvement for {consecutive_no_improvement} consecutive retries",
                        log_key="Crazy Audit Logic",
                        status="STOPPING_NO_IMPROVEMENT",
                        identifier_value=identifier['value'],
                        retry_num=retry_num
                    )
                    break
            
            previous_count = new_count
        
        # Crazy logic exhausted - cleanup and retry
        log.info(
            f"Crazy adaptive logic exhausted - cleaning up for retry",
            log_key="Crazy Audit Logic",
            status="EXHAUSTED",
            identifier_value=identifier['value'],
            total_retries_used=max_total_retries
        )
        
        return self._perform_cleaning_and_reset(
            identifier, start_time_window, end_time_window, source_count, previous_count,
            previous_count - source_count, self._calculate_difference_percentage(source_count, previous_count), 
            "not_matched"
        )
    
    def _calculate_difference_percentage(self, source_count: int, target_count: int) -> float:
        """Calculate percentage difference between source and target counts"""
        if source_count == 0 and target_count == 0:
            return 0.0
        elif source_count == 0:
            return 100.0
        else:
            return ((target_count - source_count) / source_count) * 100.0
    
    def _complete_audit_success(self, identifier: Dict[str, str], start_time_iso: str, end_time_iso: str,
                               actual_duration: float, source_count: int, target_count: int, 
                               count_difference: int, count_difference_percentage: float, 
                               match_reason: str) -> None:
        """Complete audit successfully with all details"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET audit_status = 'completed',
            audit_start_time = %(start_time)s,
            audit_end_time = %(end_time)s,
            audit_actual_duration_minutes = %(actual_duration)s,
            audit_result = %(audit_result)s,
            source_count = %(source_count)s,
            target_count = %(target_count)s,
            count_difference = %(count_difference)s,
            count_difference_percentage = %(count_difference_percentage)s,
            count_match_status = 'matched',
            completed_stage_number = 4,
            pipeline_status = 'completed',
            pipeline_end_time = %(end_time)s,
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %({identifier['column']})s
        """
        
        params = {
            'start_time': start_time_iso,
            'end_time': end_time_iso,
            'actual_duration': actual_duration,
            'audit_result': f'Audit completed successfully - {match_reason}',
            'source_count': source_count,
            'target_count': target_count,
            'count_difference': count_difference,
            'count_difference_percentage': count_difference_percentage,
            'updated_time': current_timestamp,
            identifier['column']: identifier['value']
        }
        
        self.sf_client.execute_dml_query(query, params)
    
    def _perform_cleaning_and_reset(self, identifier: Dict[str, str], start_time_window, end_time_window,
                                   source_count: int, target_count: int, count_difference: int, 
                                   count_difference_percentage: float, count_match_status: str) -> Dict[str, Any]:
        """Clean stage and target, then reset entire pipeline"""
        
        # Step 1: Perform cleaning
        clean_stage_before_ingestion(self.config, start_time_window, end_time_window)
        clean_target_before_ingestion(self.config, start_time_window, end_time_window)
        
        # Step 2: Reset entire pipeline to pending with original expected durations
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET pipeline_status = 'pending',
            pipeline_start_time = NULL,
            pipeline_end_time = NULL,
            pipeline_retry_count = pipeline_retry_count + 1,
            completed_stage_number = 0,
            
            source_discovery_status = 'pending',
            source_discovery_start_time = NULL,
            source_discovery_end_time = NULL,
            source_discovery_actual_duration_minutes = NULL,
            source_discovery_expected_duration_minutes = %(source_discovery_expected)s,
            
            source_to_stage_transfer_status = 'pending',
            source_to_stage_transfer_start_time = NULL,
            source_to_stage_transfer_end_time = NULL,
            source_to_stage_transfer_actual_duration_minutes = NULL,
            source_to_stage_transfer_expected_duration_minutes = %(source_to_stage_expected)s,
            source_to_stage_transfer_result = NULL,
            
            stage_to_target_transfer_status = 'pending',
            stage_to_target_transfer_start_time = NULL,
            stage_to_target_transfer_end_time = NULL,
            stage_to_target_transfer_actual_duration_minutes = NULL,
            stage_to_target_transfer_expected_duration_minutes = %(stage_to_target_expected)s,
            stage_to_target_transfer_result = NULL,
            
            audit_status = 'pending',
            audit_start_time = NULL,
            audit_end_time = NULL,
            audit_actual_duration_minutes = NULL,
            audit_expected_duration_minutes = %(audit_expected)s,
            audit_result = NULL,
            
            source_count = %(source_count)s,
            target_count = %(target_count)s,
            count_difference = %(count_difference)s,
            count_difference_percentage = %(count_difference_percentage)s,
            count_match_status = %(count_match_status)s,
            
            record_last_updated_time = %(updated_time)s
        WHERE {identifier['column']} = %({identifier['column']})s
        """
        
        params = {
            'source_discovery_expected': self.config.get('source_discovery_expected_duration_minutes', 3),
            'source_to_stage_expected': self.config.get('source_to_stage_transfer_expected_duration_minutes', 10),
            'stage_to_target_expected': self.config.get('stage_to_target_transfer_expected_duration_minutes', 10),
            'audit_expected': self.config.get('audit_expected_duration_minutes', 2),
            'source_count': source_count,
            'target_count': target_count,
            'count_difference': count_difference,
            'count_difference_percentage': count_difference_percentage,
            'count_match_status': count_match_status,
            'updated_time': current_timestamp,
            identifier['column']: identifier['value']
        }
        
        self.sf_client.execute_dml_query(query, params)
        
        return {
            "status": "counts_mismatch_pipeline_reset",
            "count_match_status": count_match_status,
            "source_count": source_count,
            "target_count": target_count,
            "difference_percentage": count_difference_percentage,
            "pipeline_reset": True,
            "cleaning_performed": True
        }
    
    def _get_record_info(self, identifier: Dict[str, str]) -> Dict[str, Any]:
        """Get record information for audit including stored source count"""
        query = f"""
        SELECT audit_status, source_query_window_start_time, 
               source_query_window_end_time, pipeline_retry_count,
               source_count
        FROM {self.config.get('drive_table')}
        WHERE {identifier['column']} = %({identifier['column']})s
        """
        
        params = {identifier['column']: identifier['value']}
        result = self.sf_client.fetch_all_rows_as_dataframe(query, params)
        df = result.get('data')
        
        if df.empty:
            raise ValueError(f"Record not found for {identifier['column']}: {identifier['value']}")
        
        return df.iloc[0].to_dict()
    
    def _validate_audit_status(self, record_info: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if audit can proceed"""
        status = record_info.get('audit_status')
        
        if status == 'completed':
            return {"valid": False, "reason": "already_completed"}
        elif status != 'pending':
            return {"valid": False, "reason": "not_pending"}
        else:
            return {"valid": True}
    
    def _handle_audit_failure(self, identifier: Dict[str, str], error_details: str) -> None:
        """Reset audit to pending and increment retry"""
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        query = f"""
        UPDATE {self.config.get('drive_table')}
        SET audit_status = 'pending',
            audit_start_time = NULL,
            audit_end_time = NULL,
            audit_result = NULL,
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