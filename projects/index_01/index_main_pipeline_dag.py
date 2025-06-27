# dags/main_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add project path to sys.path for imports
project_root = "/path/to/your/project"  # Update this path
sys.path.insert(0, project_root)

from pipeline_logic.core.main_pipeline_orchestrator import execute_main_pipeline
from handlers.config_handler import get_final_config

# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']  # Update with your email
}

def run_main_pipeline(**context):
    """Main pipeline execution function for Airflow"""
    
    # Extract DAG run configuration
    dag_run = context.get('dag_run')
    dag_config = dag_run.conf if dag_run and dag_run.conf else {}
    
    # Get project configuration paths from DAG config or use defaults
    root_project_path = dag_config.get('root_project_path', '/path/to/your/project')
    drive_defaults_path = dag_config.get('drive_defaults_path', 'pipeline_logic/config/drive_table_defaults.json')
    index_config_path = dag_config.get('index_config_path', 'projects/your_project/config.json')
    
    try:
        # Step 1: Load configuration
        print("🔧 Loading pipeline configuration...")
        final_config = get_final_config(
            root_project_path=root_project_path,
            drive_defaults_relative_path=drive_defaults_path,
            index_config_relative_path=index_config_path
        )
        
        # Override config with DAG parameters if provided
        if 'max_pipeline_runs' in dag_config:
            final_config['max_pipeline_runs'] = dag_config['max_pipeline_runs']
        if 'x_time_back' in dag_config:
            final_config['x_time_back'] = dag_config['x_time_back']
        if 'enable_crazy_retry_logic' in dag_config:
            final_config['enable_crazy_retry_logic'] = dag_config['enable_crazy_retry_logic']
        
        print(f"✅ Configuration loaded successfully")
        print(f"   Max pipeline runs: {final_config.get('max_pipeline_runs', 5)}")
        print(f"   Time back: {final_config.get('x_time_back', '1d')}")
        print(f"   Source: {final_config.get('source_name', 'elasticsearch')}")
        print(f"   Target: {final_config.get('target_name', 'snowflake')}")
        
        # Step 2: Execute main pipeline
        print("🚀 Starting main pipeline execution...")
        result = execute_main_pipeline(final_config)
        
        # Step 3: Process results
        status = result.get("status")
        
        if status == "batch_completed":
            summary = result.get("summary", {})
            print(f"✅ Pipeline batch completed successfully!")
            print(f"   Total processed: {summary.get('total_processed', 0)}")
            print(f"   Successful: {summary.get('successful', 0)}")
            print(f"   Failed: {summary.get('failed', 0)}")
            print(f"   Reset: {summary.get('pipeline_reset', 0)}")
            
            # Push results to XCom for downstream tasks
            context['task_instance'].xcom_push(key='pipeline_results', value=result)
            
            # Raise exception if too many failures
            failure_threshold = dag_config.get('failure_threshold', 0.5)  # 50% failure threshold
            if summary.get('total_processed', 0) > 0:
                failure_rate = summary.get('failed', 0) / summary.get('total_processed', 1)
                if failure_rate > failure_threshold:
                    raise Exception(f"High failure rate: {failure_rate:.2%} > {failure_threshold:.2%}")
            
        elif status == "valid_in_progress_exists":
            print("⏸️ Pipeline skipped - valid in_progress records exist (concurrency protection)")
            print(f"   Valid in_progress count: {result.get('valid_in_progress_count', 0)}")
            
        elif status == "no_pending_records":
            print("😴 No pending records found - nothing to process")
            
        elif status == "no_valid_records_in_time_boundary":
            print("⏰ No valid records within time boundary - all records are future data")
            print(f"   Total pending records: {result.get('total_pending_records', 0)}")
            print(f"   Time boundary: {result.get('x_time_back', 'unknown')}")
            
        elif status == "failed":
            error_msg = result.get('error', 'Unknown error')
            print(f"❌ Pipeline execution failed: {error_msg}")
            raise Exception(f"Pipeline failed: {error_msg}")
            
        else:
            print(f"⚠️ Unexpected pipeline status: {status}")
            print(f"Result: {result}")
        
        return result
        
    except Exception as e:
        print(f"💥 Pipeline execution failed with exception: {str(e)}")
        raise

def send_success_notification(**context):
    """Send success notification (customize as needed)"""
    
    # Get pipeline results from XCom
    pipeline_results = context['task_instance'].xcom_pull(key='pipeline_results')
    
    if pipeline_results and pipeline_results.get("status") == "batch_completed":
        summary = pipeline_results.get("summary", {})
        print(f"📧 Sending success notification...")
        print(f"   Pipeline completed: {summary.get('successful', 0)} successful, {summary.get('failed', 0)} failed")
        
        # Add your notification logic here (Slack, email, etc.)
        # Example: send_slack_message(summary) or send_email_report(summary)
    
    return "Notification sent"

def handle_failure_callback(context):
    """Handle DAG failure callback"""
    print(f"💥 DAG failed: {context['exception']}")
    print(f"Task: {context['task_instance'].task_id}")
    print(f"DAG: {context['dag'].dag_id}")
    print(f"Execution date: {context['execution_date']}")
    
    # Add your failure notification logic here
    # Example: send_alert_to_oncall(context) or create_incident_ticket(context)

# Create the DAG
dag = DAG(
    'main_pipeline_dag',
    default_args=default_args,
    description='Main data pipeline orchestrator - ES to S3 to Snowflake',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs
    tags=['data-pipeline', 'etl', 'elasticsearch', 'snowflake'],
    on_failure_callback=handle_failure_callback
)

# Task 1: Pipeline execution
pipeline_task = PythonOperator(
    task_id='execute_main_pipeline',
    python_callable=run_main_pipeline,
    dag=dag,
    pool='data_pipeline_pool',  # Use resource pool to limit concurrency
    pool_slots=1,
    execution_timeout=timedelta(hours=2),  # 2 hour timeout
    retries=2,
    retry_delay=timedelta(minutes=10)
)

# Task 2: Success notification (optional)
notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
    trigger_rule='none_failed_or_skipped'  # Run if pipeline task succeeded or was skipped
)

# Task 3: Cleanup (optional)
cleanup_task = DummyOperator(
    task_id='cleanup_resources',
    dag=dag,
    trigger_rule='all_done'  # Always run for cleanup
)

# Define task dependencies
pipeline_task >> notification_task >> cleanup_task