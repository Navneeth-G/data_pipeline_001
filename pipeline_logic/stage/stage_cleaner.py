# pipeline_logic/stage/stage_cleaner.py
import boto3
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log

def clean_stage_before_ingestion(config: Dict[str, Any], source_query_window_start_time, source_query_window_end_time) -> None:
    """Clean/delete existing S3 data before new ingestion"""
    
    try:
        # Build S3 path to clean (same logic as ingestion)
        from pipeline_logic.stage.ingestion_engine import ElasticsearchToS3Ingester
        
        ingester = ElasticsearchToS3Ingester(config)
        batch_details = {
            "source_query_window_start_time": source_query_window_start_time
        }
        s3_path_to_clean = ingester._build_s3_output_path(batch_details)
        
        # Parse S3 path to get bucket and key
        s3_path_parts = s3_path_to_clean.replace('s3://', '').split('/', 1)
        bucket = s3_path_parts[0]
        key = s3_path_parts[1]
        
        # Delete object from S3 if it exists
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.get('aws_access_key_id'),
            aws_secret_access_key=config.get('aws_secret_access_key'),
            region_name=config.get('s3_region', 'us-west-1')
        )
        
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            
            log.info(
                f"Cleaned existing S3 data",
                log_key="S3 Stage Cleaning",
                status="SUCCESS",
                s3_path=s3_path_to_clean
            )
            
        except Exception as delete_error:
            # Object might not exist, which is fine
            log.info(
                f"No existing S3 data to clean or deletion failed",
                log_key="S3 Stage Cleaning",
                status="NO_ACTION",
                s3_path=s3_path_to_clean,
                error_message=str(delete_error)
            )
            
    except Exception as e:
        log.error(
            f"Error during S3 stage cleaning",
            log_key="S3 Stage Cleaning",
            status="ERROR",
            error_message=str(e)
        )
        # Don't raise exception - cleaning failure shouldn't stop pipeline


