# pipeline_logic/stage/data_checker.py
import boto3
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log

def is_data_already_processed(config: Dict[str, Any], source_query_window_start_time, source_query_window_end_time) -> bool:
    """Check if data already exists in S3 for the given time window"""
    
    try:
        # Build expected S3 path (same logic as ingestion)
        from pipeline_logic.stage.ingestion_engine import ElasticsearchToS3Ingester
        
        ingester = ElasticsearchToS3Ingester(config)
        batch_details = {
            "source_query_window_start_time": source_query_window_start_time
        }
        expected_s3_path = ingester._build_s3_output_path(batch_details)
        
        # Parse S3 path to get bucket and key
        s3_path_parts = expected_s3_path.replace('s3://', '').split('/', 1)
        bucket = s3_path_parts[0]
        key = s3_path_parts[1]
        
        # Check if object exists in S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.get('aws_access_key_id'),
            aws_secret_access_key=config.get('aws_secret_access_key'),
            region_name=config.get('s3_region', 'us-west-1')
        )
        
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            
            log.info(
                f"Data already exists in S3",
                log_key="S3 Data Check",
                status="EXISTS",
                s3_path=expected_s3_path
            )
            return True
            
        except s3_client.exceptions.NoSuchKey:
            log.info(
                f"Data does not exist in S3",
                log_key="S3 Data Check",
                status="NOT_EXISTS",
                s3_path=expected_s3_path
            )
            return False
            
    except Exception as e:
        log.error(
            f"Error checking S3 data existence",
            log_key="S3 Data Check",
            status="ERROR",
            error_message=str(e)
        )
        # Return False to allow processing to continue
        return False