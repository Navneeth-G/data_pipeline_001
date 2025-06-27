# core/state_manager.py
import hashlib
import pendulum
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd

from pipeline_logic.core.concurrency_manager import ConcurrencyManager
from pipeline_logic.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_logic.utils.time_utils import generate_time_windows, get_rounded_past_timestamp, calculate_window_granularity_minutes
from pipeline_logic.utils.log_retry_decorators import log_execution_time, retry
from pipeline_logic.utils.log_generator import log
from pipeline_logic.core.source_discovery import SourceDiscoveryManager

class PipelineStateManager:
    def __init__(self, final_config: Dict[str, Any]):
        self.config = final_config
        self.sf_client = None
        self._init_snowflake_client()

        # Initialize managers
        self.concurrency_manager = ConcurrencyManager(final_config, self.sf_client)
        self.source_discovery = SourceDiscoveryManager(final_config, self.sf_client)
    
    def _init_snowflake_client(self):
        """Initialize Snowflake client with credentials from config"""
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
        
        log.info(
            "Snowflake client initialized successfully",
            log_key="State Manager Initialization",
            status="SUCCESS",
            database=snowflake_config['database'],
            schema=snowflake_config['schema'],
            table=snowflake_config['table']
        )

    # ========================================
    # TABLE MANAGEMENT
    # ========================================
    
    @log_execution_time
    @retry(max_attempts=3, delay_seconds=5)
    def create_state_table_if_not_exists(self) -> bool:
        """Create pipeline state table if it doesn't exist"""
        
        table_name = self.config.get('drive_table')
        
        if self.sf_client.table_exists(table_name):
            log.info(
                f"State table '{table_name}' already exists",
                log_key="State Table Creation",
                status="SKIPPED"
            )
            return True
        
        create_table_sql = self._generate_create_table_sql()
        result = self.sf_client.execute_control_command(create_table_sql)
        
        log.info(
            f"State table '{table_name}' created successfully",
            log_key="State Table Creation", 
            status="SUCCESS",
            query_id=result.get('query_id')
        )
        
        return True
        
    def _generate_create_table_sql(self) -> str:
        """Generate CREATE TABLE SQL with corrected schema"""
        table_name = self.config.get('drive_table')
        
        return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            unique_source_id VARCHAR(255) PRIMARY KEY,
            unique_run_id VARCHAR(255) DEFAULT NULL,
            unique_stage_id VARCHAR(500) DEFAULT NULL,
            unique_target_id VARCHAR(255) DEFAULT NULL,
            
            triggered_by VARCHAR(100) DEFAULT 'AIRFLOW',
            pipeline_name VARCHAR(100) DEFAULT 'ES_TO_S3_TO_SF',
            pipeline_priority FLOAT DEFAULT 1.2,
            pipeline_start_time TIMESTAMP_TZ DEFAULT NULL,
            pipeline_end_time TIMESTAMP_TZ DEFAULT NULL,
            pipeline_status VARCHAR(50) DEFAULT 'pending',
            pipeline_error_details TEXT DEFAULT NULL,
            pipeline_retry_count INTEGER DEFAULT 0,
            completed_stage_number INTEGER DEFAULT 0,
            record_first_inserted_time TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            record_last_updated_time TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            
            source_name VARCHAR(100) DEFAULT 'elasticsearch',
            source_category VARCHAR(200) DEFAULT NULL,
            source_subcategory VARCHAR(200) DEFAULT NULL,
            
            stage_name VARCHAR(100) DEFAULT 'aws_s3',
            stage_category VARCHAR(200) DEFAULT NULL,
            stage_subcategory VARCHAR(500) DEFAULT NULL,
            
            target_name VARCHAR(100) DEFAULT 'snowflake',
            target_category VARCHAR(300) DEFAULT NULL,
            target_subcategory VARCHAR(500) DEFAULT NULL,
            
            source_query_window_start_time TIMESTAMP_TZ DEFAULT NULL,
            source_query_window_end_time TIMESTAMP_TZ DEFAULT NULL,
            source_query_window_duration_minutes INTEGER DEFAULT NULL,
            query_window_start_day DATE DEFAULT NULL,
            
            source_discovery_is_enabled BOOLEAN DEFAULT TRUE,
            source_discovery_status VARCHAR(50) DEFAULT 'pending',
            source_discovery_start_time TIMESTAMP_TZ DEFAULT NULL,
            source_discovery_end_time TIMESTAMP_TZ DEFAULT NULL,
            source_discovery_expected_duration_minutes INTEGER DEFAULT 3,
            source_discovery_actual_duration_minutes INTEGER DEFAULT NULL,
            source_discovery_result TEXT DEFAULT NULL,
            
            source_to_stage_transfer_is_enabled BOOLEAN DEFAULT TRUE,
            source_to_stage_transfer_status VARCHAR(50) DEFAULT 'pending',
            source_to_stage_transfer_start_time TIMESTAMP_TZ DEFAULT NULL,
            source_to_stage_transfer_end_time TIMESTAMP_TZ DEFAULT NULL,
            source_to_stage_transfer_expected_duration_minutes INTEGER DEFAULT 10,
            source_to_stage_transfer_actual_duration_minutes INTEGER DEFAULT NULL,
            source_to_stage_transfer_result TEXT DEFAULT NULL,
            
            stage_to_target_transfer_is_enabled BOOLEAN DEFAULT TRUE,
            stage_to_target_transfer_status VARCHAR(50) DEFAULT 'pending',
            stage_to_target_transfer_start_time TIMESTAMP_TZ DEFAULT NULL,
            stage_to_target_transfer_end_time TIMESTAMP_TZ DEFAULT NULL,
            stage_to_target_transfer_expected_duration_minutes INTEGER DEFAULT 10,
            stage_to_target_transfer_actual_duration_minutes INTEGER DEFAULT NULL,
            stage_to_target_transfer_result TEXT DEFAULT NULL,
            
            audit_is_enabled BOOLEAN DEFAULT TRUE,
            audit_status VARCHAR(50) DEFAULT 'pending',
            audit_start_time TIMESTAMP_TZ DEFAULT NULL,
            audit_end_time TIMESTAMP_TZ DEFAULT NULL,
            audit_expected_duration_minutes INTEGER DEFAULT 2,
            audit_actual_duration_minutes INTEGER DEFAULT NULL,
            audit_result TEXT DEFAULT NULL,
            
            source_count BIGINT DEFAULT NULL,
            target_count BIGINT DEFAULT NULL,
            count_difference BIGINT DEFAULT NULL,
            count_difference_percentage FLOAT DEFAULT NULL,
            accepted_tolerance_percentage FLOAT DEFAULT 1.0,
            count_match_status VARCHAR(50) DEFAULT NULL
        )
        """

    # ========================================
    # MAIN PIPELINE LOGIC
    # ========================================
    
    @log_execution_time
    def populate_pipeline_batches(self) -> Dict[str, Any]:
        """Main function with sophisticated backfill logic"""
        
        # Step 1: Rich record analysis
        record_analysis = self._check_if_any_records_exist()
        
        if not record_analysis["records_exist"]:
            # Case 3: Fresh start - no records exist
            return self._handle_fresh_start()
        
        # Step 2: Use analysis info for smarter validation
        validation_result = self._validate_existing_days_continuity(record_analysis["analysis"])
        
        if validation_result["needs_fixes"]:
            # Use dominant duration for gap filling
            fix_result = self._fix_gaps_and_duplicates(
                validation_result["issues"], 
                record_analysis["analysis"]["dominant_duration_minutes"]
            )
        else:
            fix_result = {"status": "no_fixes_needed"}
        
        return {
            "status": "completed",
            "record_analysis": record_analysis["analysis"],
            "validation_result": validation_result,
            "fix_result": fix_result
        }

    # ========================================
    # RECORD ANALYSIS
    # ========================================
    
    def _check_if_any_records_exist(self) -> Dict[str, Any]:
        """Check if records exist AND get granularity distribution"""
        
        analysis_query = f"""
        SELECT 
            source_query_window_duration_minutes as duration_minutes,
            COUNT(*) as records_per_duration,
            MIN(query_window_start_day) as earliest_day,
            MAX(query_window_start_day) as latest_day,
            COUNT(DISTINCT query_window_start_day) as days_with_this_duration
        FROM {self.config.get('drive_table')}
        WHERE source_name = %(source_name)s
        AND source_category = %(source_category)s  
        AND source_subcategory = %(source_subcategory)s
        GROUP BY source_query_window_duration_minutes
        ORDER BY records_per_duration DESC
        """
        
        params = {
            'source_name': self.config.get('source_name'),
            'source_category': self.config.get('source_category'),
            'source_subcategory': self.config.get('source_subcategory')
        }
        
        result = self.sf_client.fetch_all_rows_as_dataframe(analysis_query, params)
        df = result.get('data')
        
        if df.empty:
            log.info(
                "No existing records found",
                log_key="Record Analysis",
                status="NO_RECORDS"
            )
            return {"records_exist": False, "analysis": None}
        
        # Rich analysis info
        total_records = df['records_per_duration'].sum()
        dominant_duration = df.iloc[0]['duration_minutes']  # Most common
        unique_granularities = len(df)
        earliest_day = str(df['earliest_day'].min())
        latest_day = str(df['latest_day'].max())
        
        log.info(
            f"Record analysis completed",
            log_key="Record Analysis",
            status="SUCCESS",
            total_records=total_records,
            unique_granularities=unique_granularities,
            dominant_duration_minutes=dominant_duration,
            date_range=f"{earliest_day} to {latest_day}"
        )
        
        return {
            "records_exist": True,
            "total_records": total_records,
            "unique_granularities": unique_granularities,
            "dominant_duration_minutes": dominant_duration,
            "earliest_day": earliest_day,
            "latest_day": latest_day,
            "granularity_distribution": df.to_dict('records')
        }

    # ========================================
    # FRESH START LOGIC
    # ========================================
    
    def _handle_fresh_start(self) -> Dict[str, Any]:
        """Case 3: No records exist - start from x_time_back"""
        
        timezone = self.config.get('timezone', 'America/Los_Angeles')
        x_time_back = self.config.get('x_time_back', '1d')
        granularity = self.config.get('granularity', '1h')
        
        # Calculate start day from x_time_back
        safe_boundary = self._calculate_safe_boundary()
        start_day = safe_boundary.start_of('day')
        
        log.info(
            f"Fresh start - generating batches from {start_day.to_date_string()}",
            log_key="Fresh Start",
            status="STARTED",
            start_day=start_day.to_date_string(),
            granularity=granularity
        )
        
        # Generate batches for this day only (x_time_back day)
        batch_count = self._generate_batches_for_date(start_day.to_date_string(), granularity)
        
        return {
            "status": "fresh_start_completed",
            "start_day": start_day.to_date_string(),
            "batches_generated": batch_count
        }

    # ========================================
    # VALIDATION LOGIC (STUB - TO BE IMPLEMENTED)
    # ========================================
    
    def _validate_existing_days_continuity(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Validate all existing days for continuity and completeness"""
        
        # TODO: Implement sophisticated continuity validation
        log.info(
            "Continuity validation - placeholder implementation",
            log_key="Continuity Validation",
            status="PLACEHOLDER"
        )
        
        return {
            "needs_fixes": False,
            "issues": {}
        }
    
    def _fix_gaps_and_duplicates(self, issues: Dict[str, Any], dominant_duration: int) -> Dict[str, Any]:
        """Fix gaps and duplicates using dominant duration"""
        
        # TODO: Implement gap and duplicate fixing
        log.info(
            "Gap and duplicate fixing - placeholder implementation",
            log_key="Gap Fixing",
            status="PLACEHOLDER"
        )
        
        return {
            "status": "no_fixes_applied"
        }

    # ========================================
    # TIME CALCULATION UTILITIES
    # ========================================
    
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
            f"Calculated safe boundary",
            log_key="Safe Boundary",
            x_time_back=x_time_back,
            current_time=current_time.to_iso8601_string(),
            safe_until=safe_until.to_iso8601_string()
        )
        
        return safe_until

    # ========================================
    # BATCH GENERATION
    # ========================================
    
    def _generate_batches_for_date(self, target_date: str, granularity: str = None) -> int:
        """Generate batches for a specific date with specified granularity"""
        
        timezone = self.config.get('timezone', 'America/Los_Angeles')
        target_date_obj = pendulum.parse(target_date, tz=timezone)
        
        # Use provided granularity or default from config
        if not granularity:
            granularity = self.config.get('granularity', '1h')
        
        # Generate time windows for this specific date
        start_of_day = target_date_obj.start_of('day')
        end_of_day = target_date_obj.end_of('day')
        
        time_windows = generate_time_windows(
            start_of_day.to_iso8601_string(),
            granularity,
            end_of_day.to_iso8601_string(),
            timezone
        )
        
        # Create batch records
        batch_records = []
        for start_time, end_time in time_windows:
            duration_minutes = calculate_window_granularity_minutes(start_time, end_time)
            batch_record = self._build_batch_record(start_time, end_time, duration_minutes)
            batch_records.append(batch_record)
        
        # Bulk insert
        if batch_records:
            df = pd.DataFrame(batch_records)
            result = self.sf_client.bulk_insert_records(df)
            
            log.info(
                f"Generated batches for date {target_date}",
                log_key="Date Batch Generation",
                status="SUCCESS",
                target_date=target_date,
                granularity=granularity,
                batch_count=len(batch_records)
            )
            
            return len(batch_records)
        
        return 0

    # ========================================
    # UNIQUE ID GENERATION
    # ========================================
    
    def _create_source_id(self, start_time: str, end_time: str) -> str:
        """Create unique source ID based on source details and time window"""
        components = {
            'source_name': self.config.get('source_name', 'elasticsearch'),
            'source_category': self._get_source_category(),
            'source_subcategory': self._get_source_subcategory(),
            'source_query_window_start_time': start_time,
            'source_query_window_end_time': end_time
        }
        
        # Sort keys for consistent hashing
        sorted_keys = sorted(components.keys())
        hash_input = "_".join([f"{key}:{components[key]}" for key in sorted_keys])
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _create_stage_id(self, start_time: str, **kwargs) -> str:
        """Create stage ID - default to S3 path pattern"""
        custom_func = kwargs.get('create_stage_id_func')
        if custom_func:
            return custom_func(start_time, **kwargs)
        
        # Default S3 path pattern
        start_time_pendulum = pendulum.parse(start_time)
        bucket_name = self.config.get('bucket_name', 'default-bucket')
        prefix = self.config.get('s3_prefix', 'data')
        index_pattern = self.config.get('index_pattern', 'logs')
        
        # Extract date/time parts
        yyyy_mm_dd = start_time_pendulum.format('YYYY-MM-DD')
        hh_mm = start_time_pendulum.format('HH-mm')
        
        return f"s3://{bucket_name}/{prefix}/{yyyy_mm_dd}/{hh_mm}/{index_pattern}_*.json"

    def _create_target_id(self, **kwargs) -> str:
        """Create target ID based on target details"""
        custom_func = kwargs.get('create_target_id_func')
        if custom_func:
            return custom_func(**kwargs)
        
        # Default hash of target category and subcategory
        components = {
            'target_category': self._get_target_category(),
            'target_subcategory': self._get_target_subcategory()
        }
        
        # Sort keys for consistent hashing
        sorted_keys = sorted(components.keys())
        hash_input = "_".join([f"{key}:{components[key]}" for key in sorted_keys])
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _create_unique_run_id(self, unique_source_id: str, unique_stage_id: str, unique_target_id: str, record_first_inserted_time: str) -> str:
        """Create unique run ID from pipeline metadata"""
        components = {
            'triggered_by': self.config.get('triggered_by', 'AIRFLOW'),
            'pipeline_name': self.config.get('pipeline_name', 'ES_TO_S3_TO_SF'),
            'pipeline_priority': str(self.config.get('pipeline_priority', 1.2)),
            'record_first_inserted_time': record_first_inserted_time,
            'unique_source_id': unique_source_id,
            'unique_stage_id': unique_stage_id,
            'unique_target_id': unique_target_id
        }
        
        # Sort keys for consistent hashing
        sorted_keys = sorted(components.keys())
        hash_input = "_".join([f"{key}:{components[key]}" for key in sorted_keys])
        return hashlib.md5(hash_input.encode()).hexdigest()

    # ========================================
    # CATEGORY FUNCTIONS
    # ========================================
    
    def _get_source_category(self, source_category_func=None):
        """Get source category with custom function fallback"""
        if source_category_func:
            result = source_category_func()
            if result:
                return result
        return self.config.get('source_category')

    def _get_source_subcategory(self, source_subcategory_func=None):
        """Get source subcategory with custom function fallback"""
        if source_subcategory_func:
            result = source_subcategory_func()
            if result:
                return result
        return self.config.get('source_subcategory')

    def _get_stage_category(self, stage_category_func=None):
        """Get stage category with custom function fallback"""
        if stage_category_func:
            result = stage_category_func()
            if result:
                return result
        return self.config.get('stage_category')

    def _get_stage_subcategory(self, stage_subcategory_func=None):
        """Get stage subcategory with custom function fallback"""
        if stage_subcategory_func:
            result = stage_subcategory_func()
            if result:
                return result
        return self.config.get('stage_subcategory')

    def _get_target_category(self, target_category_func=None):
        """Get target category with custom function fallback"""
        if target_category_func:
            result = target_category_func()
            if result:
                return result
        return self.config.get('target_category')

    def _get_target_subcategory(self, target_subcategory_func=None):
        """Get target subcategory with custom function fallback"""
        if target_subcategory_func:
            result = target_subcategory_func()
            if result:
                return result
        return self.config.get('target_subcategory')

    # ========================================
    # BATCH RECORD BUILDING
    # ========================================
    
    def _build_batch_record(self, start_time: str, end_time: str, duration_minutes: int, **kwargs) -> Dict[str, Any]:
        """Build complete batch record with modular ID generation"""
        
        current_timestamp = pendulum.now(self.config.get('timezone', 'America/Los_Angeles')).to_iso8601_string()
        
        # Extract day using pendulum
        start_time_pendulum = pendulum.parse(start_time)
        query_day = start_time_pendulum.to_date_string()  # "2025-01-15"
        
        # Step 1: Generate modular unique IDs
        unique_source_id = self._create_source_id(start_time, end_time)
        unique_stage_id = self._create_stage_id(start_time, **kwargs)
        unique_target_id = self._create_target_id(**kwargs)
        
        # Step 2: Generate unique run ID (depends on other IDs)
        unique_run_id = self._create_unique_run_id(
            unique_source_id, 
            unique_stage_id, 
            unique_target_id, 
            current_timestamp
        )
        
        # Step 3: Get categories with custom function support
        source_category = self._get_source_category(kwargs.get('source_category_func'))
        source_subcategory = self._get_source_subcategory(kwargs.get('source_subcategory_func'))
        stage_category = self._get_stage_category(kwargs.get('stage_category_func'))
        stage_subcategory = self._get_stage_subcategory(kwargs.get('stage_subcategory_func'))
        target_category = self._get_target_category(kwargs.get('target_category_func'))
        target_subcategory = self._get_target_subcategory(kwargs.get('target_subcategory_func'))
        
        return {
            # Primary key and unique identifiers
            'unique_source_id': unique_source_id,
            'unique_run_id': unique_run_id,
            'unique_stage_id': unique_stage_id,
            'unique_target_id': unique_target_id,
            
            # Pipeline metadata
            'triggered_by': self.config.get('triggered_by', 'AIRFLOW'),
            'pipeline_name': self.config.get('pipeline_name', 'ES_TO_S3_TO_SF'),
            'pipeline_priority': self.config.get('pipeline_priority', 1.2),
            'pipeline_status': 'pending',
            'completed_stage_number': 0,
            'pipeline_retry_count': 0,
            'record_first_inserted_time': current_timestamp,
            'record_last_updated_time': current_timestamp,
            
            # Source details (with modular category functions)
            'source_name': self.config.get('source_name', 'elasticsearch'),
            'source_category': source_category,
            'source_subcategory': source_subcategory,
            
            # Stage details (with modular category functions)
            'stage_name': self.config.get('stage_name', 'aws_s3'),
            'stage_category': stage_category,
            'stage_subcategory': stage_subcategory,
            
            # Target details (with modular category functions)
            'target_name': self.config.get('target_name', 'snowflake'),
            'target_category': target_category,
            'target_subcategory': target_subcategory,
            
            # Query window (with new day column)
            'source_query_window_start_time': start_time,
            'source_query_window_end_time': end_time,
            'source_query_window_duration_minutes': duration_minutes,
            'query_window_start_day': query_day,
            
            # Stage configurations
            'source_discovery_is_enabled': self.config.get('source_discovery_is_enabled', True),
            'source_discovery_status': 'pending',
            'source_discovery_expected_duration_minutes': self.config.get('source_discovery_expected_duration_minutes', 3),
            
            'source_to_stage_transfer_is_enabled': self.config.get('source_to_stage_transfer_is_enabled', True),
            'source_to_stage_transfer_status': 'pending', 
            'source_to_stage_transfer_expected_duration_minutes': self.config.get('source_to_stage_transfer_expected_duration_minutes', 10),
            
            'stage_to_target_transfer_is_enabled': self.config.get('stage_to_target_transfer_is_enabled', True),
            'stage_to_target_transfer_status': 'pending',
            'stage_to_target_transfer_expected_duration_minutes': self.config.get('stage_to_target_transfer_expected_duration_minutes', 10),
            
            'audit_is_enabled': self.config.get('audit_is_enabled', True),
            'audit_status': 'pending',
            'audit_expected_duration_minutes': self.config.get('audit_expected_duration_minutes', 2),
            
            # Audit settings
            'accepted_tolerance_percentage': self.config.get('accepted_tolerance_percentage', 1.0)
        }

    # ========================================
    # LEGACY ENTRY POINT (for backward compatibility)
    # ========================================
    
    def start_pipeline_execution(self):
        """Complete pipeline entry point with source discovery"""
        
        # Step 1: Concurrency check
        concurrency_check = self.concurrency_manager.check_running_processes()
        if concurrency_check["action"] == "exit_dag":
            return {"status": "exited", "reason": concurrency_check["status"]}
        
        # Step 2: Batch population
        batch_result = self.populate_pipeline_batches()
        
        # Step 3: Get next pending batch
        batch_selection = self.concurrency_manager.get_next_pending_batch()
        if batch_selection["action"] == "exit":
            return {"status": "no_work", "reason": "no_pending_batches"}
        
        # Step 4: Lock batch as in_progress
        selected_batch = batch_selection["batch"]
        success = self.concurrency_manager.mark_batch_as_in_progress(selected_batch["unique_source_id"])
        if not success:
            return {"status": "failed", "reason": "could_not_lock_batch"}
        
        # Step 5: Execute source discovery using your existing class
        source_discovery_result = self.source_discovery.execute_source_discovery(selected_batch["unique_source_id"])
        
        if source_discovery_result["status"] == "failed":
            return {"status": "source_discovery_failed", "error": source_discovery_result["error"]}
        
        # Step 6: Ready for next stages
        return {
            "status": "source_discovery_completed",
            "batch": selected_batch,
            "source_count": source_discovery_result["source_count"],
            "duration_estimates": source_discovery_result["duration_estimates"],
            "next_stage": source_discovery_result["next_stage"]
        }


    # Add these methods to the PipelineStateManager class

    # ========================================
    # SOPHISTICATED VALIDATION LOGIC
    # ========================================

    def _validate_existing_days_continuity(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Validate all existing days for continuity and completeness"""
        
        # Get all days that have records
        existing_days_query = f"""
        SELECT DISTINCT query_window_start_day
        FROM {self.config.get('drive_table')}
        WHERE source_name = %(source_name)s
        AND source_category = %(source_category)s  
        AND source_subcategory = %(source_subcategory)s
        AND query_window_start_day IS NOT NULL
        ORDER BY query_window_start_day ASC
        """
        
        params = {
            'source_name': self.config.get('source_name'),
            'source_category': self.config.get('source_category'),
            'source_subcategory': self.config.get('source_subcategory')
        }
        
        result = self.sf_client.fetch_all_rows_as_dataframe(existing_days_query, params)
        existing_days_df = result.get('data')
        
        if existing_days_df.empty:
            return {"needs_fixes": False, "issues": []}
        
        existing_days = [str(day) for day in existing_days_df['query_window_start_day'].tolist()]
        oldest_day = existing_days[0]
        
        # Calculate target range (oldest day to x_time_back day)
        safe_boundary = self._calculate_safe_boundary()
        target_day = safe_boundary.to_date_string()
        
        log.info(
            f"Validating continuity from {oldest_day} to {target_day}",
            log_key="Continuity Validation",
            status="STARTED",
            existing_days_count=len(existing_days)
        )
        
        # Generate expected continuous day range
        expected_days = self._generate_expected_day_range(oldest_day, target_day)
        
        # Find missing days
        missing_days = [day for day in expected_days if day not in existing_days]
        
        # For existing days, check for gaps/duplicates within each day
        problem_days = []
        for day in existing_days:
            day_issues = self._analyze_single_day(day)
            if day_issues["needs_fix"]:
                problem_days.append(day_issues)
        
        needs_fixes = len(missing_days) > 0 or len(problem_days) > 0
        
        log.info(
            f"Continuity validation completed",
            log_key="Continuity Validation",
            status="COMPLETED",
            needs_fixes=needs_fixes,
            missing_days_count=len(missing_days),
            problem_days_count=len(problem_days)
        )
        
        return {
            "needs_fixes": needs_fixes,
            "issues": {
                "missing_days": missing_days,
                "problem_days": problem_days
            },
            "oldest_day": oldest_day,
            "target_day": target_day,
            "expected_days_count": len(expected_days),
            "existing_days_count": len(existing_days)
        }

    def _generate_expected_day_range(self, start_day: str, end_day: str) -> List[str]:
        """Generate list of expected days between start and end"""
        
        timezone = self.config.get('timezone', 'America/Los_Angeles')
        start_date = pendulum.parse(start_day, tz=timezone)
        end_date = pendulum.parse(end_day, tz=timezone)
        
        expected_days = []
        current_date = start_date
        
        while current_date.date() <= end_date.date():
            expected_days.append(current_date.to_date_string())
            current_date = current_date.add(days=1)
        
        return expected_days

    def _analyze_single_day(self, target_date: str) -> Dict[str, Any]:
        """Analyze a single day for gaps and duplicates"""
        
        # Get all records for this day ordered by start time
        day_records_query = f"""
        SELECT 
            unique_source_id,
            source_query_window_start_time,
            source_query_window_end_time,
            source_query_window_duration_minutes,
            pipeline_status,
            record_last_updated_time,
            pipeline_start_time,
            record_first_inserted_time
        FROM {self.config.get('drive_table')}
        WHERE query_window_start_day = %(target_date)s
        AND source_name = %(source_name)s
        AND source_category = %(source_category)s  
        AND source_subcategory = %(source_subcategory)s
        ORDER BY source_query_window_start_time, record_last_updated_time DESC
        """
        
        params = {
            'target_date': target_date,
            'source_name': self.config.get('source_name'),
            'source_category': self.config.get('source_category'),
            'source_subcategory': self.config.get('source_subcategory')
        }
        
        result = self.sf_client.fetch_all_rows_as_dataframe(day_records_query, params)
        records_df = result.get('data')
        
        if records_df.empty:
            return {"needs_fix": False, "target_date": target_date}
        
        # Analyze for gaps and overlaps
        gaps = self._detect_time_gaps(records_df, target_date)
        duplicates = self._detect_and_prioritize_duplicates(records_df, target_date)
        
        needs_fix = len(gaps) > 0 or len(duplicates) > 0
        
        return {
            "needs_fix": needs_fix,
            "target_date": target_date,
            "gaps": gaps,
            "duplicates": duplicates,
            "total_records": len(records_df)
        }

    def _detect_time_gaps(self, records_df: pd.DataFrame, target_date: str) -> List[Dict[str, Any]]:
        """Detect gaps in time coverage for a day"""
        
        if records_df.empty:
            return []
        
        timezone = self.config.get('timezone', 'America/Los_Angeles')
        target_date_obj = pendulum.parse(target_date, tz=timezone)
        
        # Expected day boundaries
        start_of_day = target_date_obj.start_of('day')
        end_of_day = target_date_obj.end_of('day')
        
        gaps = []
        
        # Convert timestamps to pendulum for comparison
        records_df['start_pendulum'] = records_df['source_query_window_start_time'].apply(
            lambda x: pendulum.parse(str(x))
        )
        records_df['end_pendulum'] = records_df['source_query_window_end_time'].apply(
            lambda x: pendulum.parse(str(x))
        )
        
        # Sort by start time
        records_df = records_df.sort_values('start_pendulum')
        
        # Check gap from start of day to first record
        first_record_start = records_df.iloc[0]['start_pendulum']
        if first_record_start > start_of_day:
            gaps.append({
                "gap_start": start_of_day.to_iso8601_string(),
                "gap_end": first_record_start.to_iso8601_string(),
                "gap_type": "start_of_day"
            })
        
        # Check gaps between consecutive records
        for i in range(len(records_df) - 1):
            current_end = records_df.iloc[i]['end_pendulum']
            next_start = records_df.iloc[i + 1]['start_pendulum']
            
            if next_start > current_end:
                gaps.append({
                    "gap_start": current_end.to_iso8601_string(),
                    "gap_end": next_start.to_iso8601_string(),
                    "gap_type": "between_records"
                })
        
        # Check gap from last record to end of day
        last_record_end = records_df.iloc[-1]['end_pendulum']
        if last_record_end < end_of_day:
            gaps.append({
                "gap_start": last_record_end.to_iso8601_string(),
                "gap_end": end_of_day.to_iso8601_string(),
                "gap_type": "end_of_day"
            })
        
        log.info(
            f"Detected {len(gaps)} time gaps for {target_date}",
            log_key="Gap Detection",
            status="COMPLETED",
            target_date=target_date,
            gaps_count=len(gaps)
        )
        
        return gaps

    def _detect_and_prioritize_duplicates(self, records_df: pd.DataFrame, target_date: str) -> List[Dict[str, Any]]:
        """Detect overlapping time windows and prioritize which to keep"""
        
        if len(records_df) <= 1:
            return []
        
        duplicates = []
        
        # Convert timestamps to pendulum for comparison
        records_df['start_pendulum'] = records_df['source_query_window_start_time'].apply(
            lambda x: pendulum.parse(str(x))
        )
        records_df['end_pendulum'] = records_df['source_query_window_end_time'].apply(
            lambda x: pendulum.parse(str(x))
        )
        
        # Sort by start time
        records_df = records_df.sort_values('start_pendulum')
        
        # Check for overlaps
        for i in range(len(records_df) - 1):
            current_record = records_df.iloc[i]
            next_record = records_df.iloc[i + 1]
            
            current_end = current_record['end_pendulum']
            next_start = next_record['start_pendulum']
            
            # If next starts before current ends = overlap
            if next_start < current_end:
                
                # Determine which record to keep using priority rules
                keep_record, delete_record = self._prioritize_duplicate_records(current_record, next_record)
                
                duplicates.append({
                    "overlap_start": next_start.to_iso8601_string(),
                    "overlap_end": min(current_end, next_record['end_pendulum']).to_iso8601_string(),
                    "keep_record": keep_record['unique_source_id'],
                    "delete_record": delete_record['unique_source_id'],
                    "priority_reason": self._get_priority_reason(keep_record, delete_record)
                })
        
        log.info(
            f"Detected {len(duplicates)} overlapping records for {target_date}",
            log_key="Duplicate Detection",
            status="COMPLETED",
            target_date=target_date,
            duplicates_count=len(duplicates)
        )
        
        return duplicates

    def _prioritize_duplicate_records(self, record1: pd.Series, record2: pd.Series) -> tuple:
        """Prioritize which duplicate record to keep based on business rules"""
        
        # Priority 1: Completed status wins over non-completed
        if record1['pipeline_status'] == 'completed' and record2['pipeline_status'] != 'completed':
            return record1, record2
        elif record2['pipeline_status'] == 'completed' and record1['pipeline_status'] != 'completed':
            return record2, record1
        
        # Priority 2: If both completed, use latest record_last_updated_time
        if record1['pipeline_status'] == 'completed' and record2['pipeline_status'] == 'completed':
            record1_updated = pendulum.parse(str(record1['record_last_updated_time']))
            record2_updated = pendulum.parse(str(record2['record_last_updated_time']))
            
            if record1_updated > record2_updated:
                return record1, record2
            elif record2_updated > record1_updated:
                return record2, record1
            else:
                # Same update time, use pipeline_start_time
                if pd.notna(record1['pipeline_start_time']) and pd.notna(record2['pipeline_start_time']):
                    record1_started = pendulum.parse(str(record1['pipeline_start_time']))
                    record2_started = pendulum.parse(str(record2['pipeline_start_time']))
                    
                    if record1_started > record2_started:
                        return record1, record2
                    elif record2_started > record1_started:
                        return record2, record1
                
                # Final tiebreaker: record_first_inserted_time (keep older)
                record1_inserted = pendulum.parse(str(record1['record_first_inserted_time']))
                record2_inserted = pendulum.parse(str(record2['record_first_inserted_time']))
                
                if record1_inserted < record2_inserted:
                    return record1, record2
                else:
                    return record2, record1
        
        # Priority 3: Neither completed - use latest record_last_updated_time
        record1_updated = pendulum.parse(str(record1['record_last_updated_time']))
        record2_updated = pendulum.parse(str(record2['record_last_updated_time']))
        
        if record1_updated > record2_updated:
            return record1, record2
        else:
            return record2, record1

    def _get_priority_reason(self, keep_record: pd.Series, delete_record: pd.Series) -> str:
        """Get human-readable reason for priority decision"""
        
        if keep_record['pipeline_status'] == 'completed' and delete_record['pipeline_status'] != 'completed':
            return "kept_completed_over_incomplete"
        elif keep_record['pipeline_status'] == 'completed' and delete_record['pipeline_status'] == 'completed':
            return "kept_latest_completed"
        else:
            return "kept_latest_updated"

    # ========================================
    # GAP AND DUPLICATE FIXING
    # ========================================

    def _fix_gaps_and_duplicates(self, issues: Dict[str, Any], dominant_duration: int) -> Dict[str, Any]:
        """Fix gaps and duplicates using dominant duration"""
        
        missing_days = issues.get("missing_days", [])
        problem_days = issues.get("problem_days", [])
        
        fix_results = {
            "missing_days_fixed": 0,
            "gaps_filled": 0,
            "duplicates_removed": 0,
            "total_records_added": 0,
            "total_records_deleted": 0
        }
        
        # Fix missing days
        for missing_day in missing_days:
            # Use dominant duration for missing days
            granularity = self._duration_to_granularity(dominant_duration)
            batch_count = self._generate_batches_for_date(missing_day, granularity)
            fix_results["missing_days_fixed"] += 1
            fix_results["total_records_added"] += batch_count
        
        # Fix problem days
        for problem_day in problem_days:
            day_fix_result = self._fix_single_day_issues(problem_day, dominant_duration)
            fix_results["gaps_filled"] += len(day_fix_result.get("gaps_filled", []))
            fix_results["duplicates_removed"] += len(day_fix_result.get("duplicates_removed", []))
            fix_results["total_records_added"] += day_fix_result.get("records_added", 0)
            fix_results["total_records_deleted"] += day_fix_result.get("records_deleted", 0)
        
        log.info(
            f"Gap and duplicate fixing completed",
            log_key="Gap Fixing",
            status="COMPLETED",
            fix_results=fix_results
        )
        
        return {
            "status": "fixes_applied",
            "fix_results": fix_results
        }

    def _fix_single_day_issues(self, problem_day: Dict[str, Any], dominant_duration: int) -> Dict[str, Any]:
        """Fix gaps and duplicates for a single day"""
        
        target_date = problem_day["target_date"]
        gaps = problem_day.get("gaps", [])
        duplicates = problem_day.get("duplicates", [])
        
        fix_result = {
            "gaps_filled": [],
            "duplicates_removed": [],
            "records_added": 0,
            "records_deleted": 0
        }
        
        # Fill gaps
        for gap in gaps:
            gap_records = self._fill_time_gap(gap, dominant_duration)
            fix_result["gaps_filled"].append(gap)
            fix_result["records_added"] += len(gap_records)
        
        # Remove duplicates
        for duplicate in duplicates:
            delete_success = self._remove_duplicate_record(duplicate["delete_record"])
            if delete_success:
                fix_result["duplicates_removed"].append(duplicate)
                fix_result["records_deleted"] += 1
        
        return fix_result

    def _fill_time_gap(self, gap: Dict[str, Any], dominant_duration: int) -> List[Dict[str, Any]]:
        """Fill a specific time gap using dominant duration"""
        
        gap_start = gap["gap_start"]
        gap_end = gap["gap_end"]
        granularity = self._duration_to_granularity(dominant_duration)
        
        # Generate time windows for the gap
        time_windows = generate_time_windows(
            gap_start,
            granularity,
            gap_end,
            self.config.get('timezone', 'America/Los_Angeles')
        )
        
        # Create batch records for the gap
        gap_records = []
        for start_time, end_time in time_windows:
            duration_minutes = calculate_window_granularity_minutes(start_time, end_time)
            batch_record = self._build_batch_record(start_time, end_time, duration_minutes)
            gap_records.append(batch_record)
        
        # Insert gap records
        if gap_records:
            df = pd.DataFrame(gap_records)
            result = self.sf_client.bulk_insert_records(df)
            
            log.info(
                f"Filled time gap with {len(gap_records)} records",
                log_key="Gap Filling",
                status="SUCCESS",
                gap_start=gap_start,
                gap_end=gap_end,
                records_added=len(gap_records)
            )
        
        return gap_records

    def _remove_duplicate_record(self, unique_source_id: str) -> bool:
        """Remove a duplicate record"""
        
        delete_query = f"""
        DELETE FROM {self.config.get('drive_table')}
        WHERE unique_source_id = %(unique_source_id)s
        """
        
        try:
            result = self.sf_client.execute_dml_query(delete_query, {"unique_source_id": unique_source_id})
            rows_deleted = result.get('rows_affected', 0)
            
            log.info(
                f"Removed duplicate record",
                log_key="Duplicate Removal",
                status="SUCCESS",
                unique_source_id=unique_source_id,
                rows_deleted=rows_deleted
            )
            
            return rows_deleted > 0
            
        except Exception as e:
            log.error(
                f"Failed to remove duplicate record",
                log_key="Duplicate Removal",
                status="FAILED",
                unique_source_id=unique_source_id,
                error_message=str(e)
            )
            return False

    def _duration_to_granularity(self, duration_minutes: int) -> str:
        """Convert duration minutes back to granularity format"""
        if duration_minutes >= 1440:  # 24 hours or more
            days = duration_minutes // 1440
            return f"{days}d"
        elif duration_minutes >= 60:  # 1 hour or more  
            hours = duration_minutes // 60
            remainder = duration_minutes % 60
            if remainder == 0:
                return f"{hours}h"
            else:
                return f"{hours}h{remainder}m"
        else:  # Less than 1 hour
            return f"{duration_minutes}m"



























