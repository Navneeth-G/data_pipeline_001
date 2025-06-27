# pipeline_logic/core/main_pipeline_orchestrator.py
import pendulum
from typing import Dict, Any, List
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_logic.core.state_manager import PipelineStateManager
from pipeline_logic.core.progress_validator import InProgressRecordsValidator
from pipeline_logic.core.invalid_progress_handler import InvalidProgressHandler
from pipeline_logic.core.pending_records_selector import PendingRecordsSelector
from pipeline_logic.core.record_status_updater import RecordStatusUpdater
from pipeline_logic.core.source_discovery_manager import SourceDiscoveryManager
from pipeline_logic.core.source_to_stage_ingestion_manager import SourceToStageIngestionManager
from pipeline_logic.core.stage_to_target_ingestion_manager import StageToTargetIngestionManager
from pipeline_logic.core.audit_manager import AuditManager

class MainPipelineOrchestrator:
    def __init__(self, final_config: Dict[str, Any]):
        self.config = final_config
        self.sf_client = None
        self._init_snowflake_client()
        
        # Initialize all managers
        self.state_manager = PipelineStateManager(final_config)
        self.progress_validator = InProgressRecordsValidator(final_config, self.sf_client)
        self.invalid_handler = InvalidProgressHandler(final_config, self.sf_client)
        self.pending_selector = PendingRecordsSelector(final_config, self.sf_client)
        self.status_updater = RecordStatusUpdater(final_config, self.sf_client)
        self.source_discovery = SourceDiscoveryManager(final_config, self.sf_client)
        self.source_to_stage = SourceToStageIngestionManager(final_config, self.sf_client)
        self.stage_to_target = StageToTargetIngestionManager(final_config, self.sf_client)
        self.audit_manager = AuditManager(final_config, self.sf_client)
    
    def _init_snowflake_client(self):
        """Initialize Snowflake client"""
        snowflake_creds = {
            'account': self.config.get('sf_account'),
            'user': self.config.get('sf_username'), 
            'password': self.config.get('sf_password'),
            'role': self.config.get('sf_role'),
            'warehouse': self.config.get('sf_warehouse')
        }
        
        snowflake_config = {
            'database': self.config.get('drive_database'),
            'schema': self.config.get('drive_schema'),
            'table': self.config.get('drive_table')
        }
        
        self.sf_client = SnowflakeQueryClient(snowflake_creds, snowflake_config)
    
    def execute_main_pipeline(self) -> Dict[str, Any]:
        """Main pipeline orchestration with complete workflow"""
        
        log.info(
            f"Starting main pipeline execution",
            log_key="Main Pipeline",
            status="STARTED"
        )
        
        try:
            # Step 0: ALWAYS populate/validate drive table first
            log.info(
                f"Step 0: Starting drive table population and validation",
                log_key="Main Pipeline",
                status="DRIVE_TABLE_POPULATION_STARTED"
            )
            
            populate_result = self.state_manager.populate_pipeline_batches()
            
            log.info(
                f"Step 0: Drive table population completed",
                log_key="Main Pipeline",
                status="DRIVE_TABLE_POPULATION_COMPLETED",
                populate_result_status=populate_result.get("status")
            )
            
            # Step 1: Validate and handle invalid in_progress records
            log.info(
                f"Step 1: Starting progress validation",
                log_key="Main Pipeline",
                status="PROGRESS_VALIDATION_STARTED"
            )
            
            validation_result = self.progress_validator.validate_all_in_progress_records()
            
            if validation_result.get("status") == "no_in_progress_records":
                log.info(
                    f"No in_progress records found - proceeding with pipeline",
                    log_key="Main Pipeline",
                    status="NO_IN_PROGRESS"
                )
            else:
                # Handle invalid in_progress records
                invalid_records = validation_result.get("invalid_records", [])
                valid_records = validation_result.get("valid_records", [])
                
                if invalid_records:
                    log.info(
                        f"Found {len(invalid_records)} invalid in_progress records - resetting them",
                        log_key="Main Pipeline",
                        status="HANDLING_INVALID",
                        invalid_count=len(invalid_records)
                    )
                    
                    reset_result = self.invalid_handler.reset_invalid_records(invalid_records)
                    
                    log.info(
                        f"Invalid records handled",
                        log_key="Main Pipeline",
                        status="INVALID_HANDLED",
                        successful_resets=len(reset_result.get("successful_resets", [])),
                        failed_resets=len(reset_result.get("failed_resets", []))
                    )
                
                # Step 2: CONCURRENCY PROTECTION - Check for valid in_progress records
                if valid_records:
                    log.info(
                        f"Found {len(valid_records)} valid in_progress records - exiting for concurrency protection",
                        log_key="Main Pipeline",
                        status="CONCURRENCY_EXIT",
                        valid_in_progress_count=len(valid_records)
                    )
                    
                    return {
                        "status": "valid_in_progress_exists",
                        "exit_reason": "concurrency_protection",
                        "valid_in_progress_count": len(valid_records),
                        "message": "Pipeline exited to avoid concurrency conflicts"
                    }
            
            # Step 3: Get pending batches to process
            log.info(
                f"Step 3: Getting pending records for processing",
                log_key="Main Pipeline",
                status="GETTING_PENDING_RECORDS"
            )
            
            max_pipeline_runs = self.config.get('max_pipeline_runs', 5)
            pending_records = self.pending_selector.get_pending_records(max_pipeline_runs)
            
            if pending_records.empty:
                log.info(
                    f"No pending records found - nothing to process",
                    log_key="Main Pipeline",
                    status="NO_PENDING_WORK"
                )
                
                return {
                    "status": "no_pending_records",
                    "message": "No work available for processing"
                }
            
            # Step 4: Filter records by time boundary
            log.info(
                f"Step 4: Filtering records by time boundary",
                log_key="Main Pipeline",
                status="TIME_BOUNDARY_FILTERING",
                total_pending_records=len(pending_records)
            )
            
            valid_records = self._filter_records_by_time_boundary(pending_records)
            
            if valid_records.empty:
                log.info(
                    f"No valid records within time boundary - all records are future data",
                    log_key="Main Pipeline",
                    status="NO_VALID_RECORDS_IN_TIME_BOUNDARY",
                    total_pending=len(pending_records),
                    x_time_back=self.config.get('x_time_back', '1d')
                )
                
                return {
                    "status": "no_valid_records_in_time_boundary",
                    "message": f"All {len(pending_records)} pending records are beyond x_time_back boundary",
                    "total_pending_records": len(pending_records),
                    "x_time_back": self.config.get('x_time_back', '1d')
                }
            
            # Step 5: Process each valid record in the batch
            log.info(
                f"Step 5: Starting batch processing",
                log_key="Main Pipeline",
                status="BATCH_PROCESSING_STARTED",
                total_valid_records=len(valid_records),
                total_pending_records=len(pending_records),
                filtered_future_records=len(pending_records) - len(valid_records),
                max_pipeline_runs=max_pipeline_runs
            )
            
            batch_results = self._process_record_batch(valid_records)
            
            return batch_results
            
        except Exception as e:
            log.error(
                f"Main pipeline execution failed",
                log_key="Main Pipeline",
                status="FAILED",
                error_message=str(e)
            )
            
            return {
                "status": "failed",
                "error": str(e)
            }
        finally:
            if self.sf_client:
                self.sf_client.close_connection()
    
    def _calculate_safe_boundary(self) -> pendulum.DateTime:
        """Calculate safe time boundary based on x_time_back"""
        
        x_time_back = self.config.get('x_time_back', '1d')
        timezone = self.config.get('timezone', 'America/Los_Angeles')
        
        current_time = pendulum.now(timezone)
        
        # Parse time back (1d, 1h, 20m, 30m)
        if 'd' in x_time_back:
            days = int(x_time_back.replace('d', ''))
            safe_until = current_time.subtract(days=days).start_of('day')
        elif 'h' in x_time_back:
            hours = int(x_time_back.replace('h', ''))
            safe_until = current_time.subtract(hours=hours).start_of('hour')
        elif 'm' in x_time_back:
            minutes = int(x_time_back.replace('m', ''))
            safe_until = current_time.subtract(minutes=minutes).start_of('minute')
        else:
            # Default fallback
            safe_until = current_time.subtract(days=1).start_of('day')
        
        log.info(
            f"Calculated safe boundary for processing",
            log_key="Time Boundary",
            x_time_back=x_time_back,
            current_time=current_time.to_iso8601_string(),
            safe_until=safe_until.to_iso8601_string()
        )
        
        return safe_until
    
    def _filter_records_by_time_boundary(self, pending_records) -> pd.DataFrame:
        """Filter out records that are beyond x_time_back boundary (future data)"""
        
        if pending_records.empty:
            return pending_records
        
        safe_boundary = self._calculate_safe_boundary()
        
        # For hour/minute precision, compare actual timestamps
        x_time_back = self.config.get('x_time_back', '1d')
        
        if 'h' in x_time_back or 'm' in x_time_back:
            # Use timestamp comparison for hour/minute precision
            safe_boundary_timestamp = safe_boundary.to_iso8601_string()
            
            # Convert timestamps to string for comparison
            pending_records['start_time_str'] = pending_records['source_query_window_start_time'].astype(str)
            
            valid_records = pending_records[
                pending_records['start_time_str'] <= safe_boundary_timestamp
            ].copy()
            
            # Remove temporary column
            valid_records = valid_records.drop('start_time_str', axis=1)
        else:
            # Use date comparison for day precision
            safe_boundary_date_str = safe_boundary.to_date_string()
            
            # Convert query_window_start_day to string for comparison
            pending_records['query_window_start_day_str'] = pending_records['query_window_start_day'].astype(str)
            
            valid_records = pending_records[
                pending_records['query_window_start_day_str'] <= safe_boundary_date_str
            ].copy()
            
            # Remove temporary column
            valid_records = valid_records.drop('query_window_start_day_str', axis=1)
        
        filtered_count = len(pending_records) - len(valid_records)
        
        if filtered_count > 0:
            log.info(
                f"Filtered out {filtered_count} future records beyond time boundary",
                log_key="Time Boundary Filter",
                status="FUTURE_RECORDS_FILTERED",
                total_pending_records=len(pending_records),
                valid_records=len(valid_records),
                filtered_count=filtered_count,
                safe_boundary=safe_boundary.to_iso8601_string()
            )
        else:
            log.info(
                f"All {len(pending_records)} records are within time boundary",
                log_key="Time Boundary Filter",
                status="ALL_RECORDS_VALID",
                safe_boundary=safe_boundary.to_iso8601_string()
            )
        
        return valid_records
    
    def _process_record_batch(self, pending_records) -> Dict[str, Any]:
        """Process each record in the batch through all 4 stages"""
        
        successful_records = []
        failed_records = []
        pipeline_reset_records = []
        
        total_records = len(pending_records)
        
        # Loop through each pending record
        for index, (_, record) in enumerate(pending_records.iterrows(), 1):
            unique_run_id = record.get("unique_run_id")
            unique_source_id = record.get("unique_source_id")
            
            log.info(
                f"Processing record {index}/{total_records}",
                log_key="Batch Processing",
                status="RECORD_STARTED",
                record_index=index,
                total_records=total_records,
                unique_run_id=unique_run_id,
                unique_source_id=unique_source_id
            )
            
            try:
                # Process this single record through all 4 stages
                record_result = self._execute_single_record_pipeline(unique_run_id, unique_source_id, index)
                
                # Categorize result
                if record_result["status"] == "completed":
                    successful_records.append(record_result)
                    log.info(
                        f"Record {index}/{total_records} completed successfully",
                        log_key="Batch Processing",
                        status="RECORD_SUCCESS",
                        record_index=index,
                        unique_run_id=unique_run_id
                    )
                elif record_result["status"] == "pipeline_reset":
                    pipeline_reset_records.append(record_result)
                    log.info(
                        f"Record {index}/{total_records} reset due to count mismatch",
                        log_key="Batch Processing",
                        status="RECORD_RESET",
                        record_index=index,
                        unique_run_id=unique_run_id
                    )
                else:
                    failed_records.append(record_result)
                    log.warning(
                        f"Record {index}/{total_records} failed",
                        log_key="Batch Processing",
                        status="RECORD_FAILED",
                        record_index=index,
                        unique_run_id=unique_run_id,
                        error=record_result.get("error")
                    )
                    
            except Exception as e:
                # Record processing failed - continue with next record
                failed_record = {
                    "status": "failed",
                    "unique_run_id": unique_run_id,
                    "unique_source_id": unique_source_id,
                    "error": str(e),
                    "record_index": index
                }
                failed_records.append(failed_record)
                
                log.error(
                    f"Record {index}/{total_records} processing failed - continuing with next record",
                    log_key="Batch Processing",
                    status="RECORD_ERROR",
                    record_index=index,
                    unique_run_id=unique_run_id,
                    error_message=str(e)
                )
        
        # Return batch processing summary
        log.info(
            f"Batch processing completed",
            log_key="Batch Processing",
            status="BATCH_COMPLETED",
            total_processed=total_records,
            successful=len(successful_records),
            failed=len(failed_records),
            reset=len(pipeline_reset_records)
        )
        
        return {
            "status": "batch_completed",
            "summary": {
                "total_processed": total_records,
                "successful": len(successful_records),
                "failed": len(failed_records),
                "pipeline_reset": len(pipeline_reset_records)
            },
            "results": {
                "successful_records": successful_records,
                "failed_records": failed_records,
                "pipeline_reset_records": pipeline_reset_records
            }
        }
    
    def _execute_single_record_pipeline(self, unique_run_id: str, unique_source_id: str, record_index: int) -> Dict[str, Any]:
        """Execute all pipeline stages for a single record"""
        
        try:
            # Start pipeline execution
            self.status_updater.start_pipeline_execution(unique_run_id, unique_source_id)
            
            # Stage 1: Source Discovery
            log.info(
                f"Record {record_index}: Starting Stage 1 - Source Discovery",
                log_key="Single Record Pipeline",
                status="STAGE_1_STARTED",
                unique_run_id=unique_run_id,
                record_index=record_index
            )
            
            discovery_result = self.source_discovery.execute_source_discovery(unique_run_id, unique_source_id)
            
            if discovery_result["status"] != "completed":
                return {
                    "status": "failed",
                    "failed_stage": "source_discovery",
                    "unique_run_id": unique_run_id,
                    "unique_source_id": unique_source_id,
                    "record_index": record_index,
                    "error": discovery_result.get("error", "Source discovery failed")
                }
            
            # Stage 2: Source to Stage Ingestion
            log.info(
                f"Record {record_index}: Starting Stage 2 - Source to Stage Ingestion",
                log_key="Single Record Pipeline",
                status="STAGE_2_STARTED",
                unique_run_id=unique_run_id,
                record_index=record_index
            )
            
            source_to_stage_result = self.source_to_stage.execute_source_to_stage_ingestion(unique_run_id, unique_source_id)
            
            if source_to_stage_result["status"] != "completed":
                return {
                    "status": "failed",
                    "failed_stage": "source_to_stage_transfer",
                    "unique_run_id": unique_run_id,
                    "unique_source_id": unique_source_id,
                    "record_index": record_index,
                    "error": source_to_stage_result.get("error", "Source to stage ingestion failed")
                }
            
            # Stage 3: Stage to Target Ingestion
            log.info(
                f"Record {record_index}: Starting Stage 3 - Stage to Target Ingestion",
                log_key="Single Record Pipeline",
                status="STAGE_3_STARTED",
                unique_run_id=unique_run_id,
                record_index=record_index
            )
            
            stage_to_target_result = self.stage_to_target.execute_stage_to_target_ingestion(unique_run_id, unique_source_id)
            
            if stage_to_target_result["status"] != "completed":
                return {
                    "status": "failed",
                    "failed_stage": "stage_to_target_transfer",
                    "unique_run_id": unique_run_id,
                    "unique_source_id": unique_source_id,
                    "record_index": record_index,
                    "error": stage_to_target_result.get("error", "Stage to target ingestion failed")
                }
            
            # Stage 4: Audit
            log.info(
                f"Record {record_index}: Starting Stage 4 - Audit",
                log_key="Single Record Pipeline",
                status="STAGE_4_STARTED",
                unique_run_id=unique_run_id,
                record_index=record_index
            )
            
            audit_result = self.audit_manager.execute_audit_process(unique_run_id, unique_source_id)
            
            if audit_result["status"] == "completed":
                return {
                    "status": "completed",
                    "unique_run_id": unique_run_id,
                    "unique_source_id": unique_source_id,
                    "record_index": record_index,
                    "source_count": audit_result.get("source_count"),
                    "target_count": audit_result.get("target_count"),
                    "count_match_status": audit_result.get("count_match_status"),
                    "retry_attempts_used": audit_result.get("retry_attempts_used", 0)
                }
            elif audit_result["status"] == "counts_mismatch_pipeline_reset":
                return {
                    "status": "pipeline_reset",
                    "reason": "count_mismatch",
                    "unique_run_id": unique_run_id,
                    "unique_source_id": unique_source_id,
                    "record_index": record_index,
                    "source_count": audit_result.get("source_count"),
                    "target_count": audit_result.get("target_count"),
                    "cleaning_performed": audit_result.get("cleaning_performed")
                }
            else:
                return {
                    "status": "failed",
                    "failed_stage": "audit",
                    "unique_run_id": unique_run_id,
                    "unique_source_id": unique_source_id,
                    "record_index": record_index,
                    "error": audit_result.get("error", "Audit process failed")
                }
                
        except Exception as e:
            # Mark pipeline as failed
            self.status_updater.fail_pipeline(str(e), unique_run_id, unique_source_id)
            
            return {
                "status": "failed",
                "unique_run_id": unique_run_id,
                "unique_source_id": unique_source_id,
                "record_index": record_index,
                "error": str(e)
            }

# Standalone function for easy usage
def execute_main_pipeline(final_config: Dict[str, Any]) -> Dict[str, Any]:
    """Standalone function to execute main pipeline"""
    orchestrator = MainPipelineOrchestrator(final_config)
    return orchestrator.execute_main_pipeline()