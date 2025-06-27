# pipeline_logic/stage/ingestion_engine.py
import json
import subprocess
import pendulum
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.log_retry_decorators import retry
from pipeline_logic.utils.time_utils import normalize_timestamp_to_iso8601

class ElasticsearchToS3Ingester:
    def __init__(self, final_config: Dict[str, Any]):
        self.config = final_config
    
    def execute_ingestion(self, source_query_window_start_time, source_query_window_end_time) -> Dict[str, Any]:
        """Execute Elasticsearch to S3 ingestion using elasticdump"""
        
        # Convert timestamps to ES format
        start_time_es = self._convert_timestamp_to_es_format(source_query_window_start_time)
        end_time_es = self._convert_timestamp_to_es_format(source_query_window_end_time)
        
        # Build batch details for processing
        batch_details = {
            "query_start_ts_str": start_time_es,
            "query_end_ts_str": end_time_es,
            "source_query_window_start_time": source_query_window_start_time
        }
        
        try:
            # Step 1: Build S3 output path
            output_s3_path = self._build_s3_output_path(batch_details)
            
            # Step 2: Execute elasticdump
            self._execute_elasticdump(batch_details, output_s3_path)
            
            return {
                "status": "success",
                "output_s3_path": output_s3_path,
                "start_time": start_time_es,
                "end_time": end_time_es
            }
            
        except Exception as e:
            log.error(
                f"Elasticsearch to S3 ingestion failed",
                log_key="ES to S3 Ingestion",
                status="FAILED",
                start_time=start_time_es,
                end_time=end_time_es,
                error_message=str(e)
            )
            raise
    
    def _convert_timestamp_to_es_format(self, timestamp):
        """Convert timestamp to Elasticsearch format using configurable format"""
        # Use existing utility to get ISO format first
        iso_timestamp = normalize_timestamp_to_iso8601(timestamp)
        # Then convert to ES format using strftime
        parsed_time = pendulum.parse(iso_timestamp)
        es_format = self.config.get('es_timestamp_format', "%Y-%m-%dT%H:%M:%SZ")
        return parsed_time.strftime(es_format)
    
    def _build_s3_output_path(self, batch_details: Dict[str, Any]) -> str:
        """Build S3 output path for elasticdump"""
        
        # Extract date/time parts for path structure
        start_time = batch_details["source_query_window_start_time"]
        if isinstance(start_time, str):
            start_time = pendulum.parse(start_time)
        elif hasattr(start_time, 'timestamp'):
            # Handle pandas timestamp or other timestamp types
            iso_timestamp = normalize_timestamp_to_iso8601(start_time)
            start_time = pendulum.parse(iso_timestamp)
        
        # Use config to build S3 path
        bucket = self.config.get('bucket_name')
        s3_prefix = self.config.get('s3_prefix', '')
        file_prefix = self.config.get('file_prefix', 'data')
        
        # Replace placeholders in s3_prefix if needed
        env = self.config.get('env', 'dev')
        index_group = self.config.get('index_group', '')
        index_name = self.config.get('index_name', '')
        
        s3_prefix_resolved = s3_prefix.replace('{env}', env).replace('{index_group}', index_group).replace('{index_name}', index_name)
        
        # Build full S3 path
        date_path = start_time.format('YYYY-MM-DD/HH-mm')
        epoch_timestamp = int(start_time.timestamp())
        
        s3_path = f"s3://{bucket}/{s3_prefix_resolved}/{date_path}/{file_prefix}_{epoch_timestamp}.json"
        
        log.info(
            f"Built S3 output path",
            log_key="S3 Path Generation",
            status="SUCCESS",
            s3_path=s3_path
        )
        
        return s3_path
    
    @retry(max_attempts=3, delay_seconds=30)
    def _execute_elasticdump(self, batch_details: Dict[str, Any], output_s3_path: str):
        """Execute elasticdump command using legacy production logic"""
        
        # Build index config from our config (mapping to legacy format)
        index_config = {
            "es_timezone": self.config.get('timezone', 'America/Los_Angeles'),
            "index_name": self.config.get('index_pattern'),
            "index_host_url": f"http://{self.config.get('host_name')}:{self.config.get('port')}/{self.config.get('index_pattern')}",
            "ca_certs_path": self.config.get('es_ca_certs_path'),
            "header": json.loads(self.config.get('es_header', '{}')),
            "timestamp_field_name": self.config.get('timestamp_field', '@timestamp'),
            "aws_access_key": self.config.get('aws_access_key_id'),
            "aws_secret_key": self.config.get('aws_secret_access_key')
        }
        
        # Call the legacy function (adapted)
        self._es_to_s3_elt(
            index_config=index_config,
            output_s3_path=output_s3_path,
            query_start_ts_str=batch_details["query_start_ts_str"],
            query_end_ts_str=batch_details["query_end_ts_str"]
        )
        
        log.info(
            f"Elasticdump execution completed",
            log_key="Elasticdump Execution",
            status="SUCCESS",
            output_s3_path=output_s3_path
        )
    
    def _es_to_s3_elt(self, index_config: Dict[str, Any], output_s3_path: str, query_start_ts_str: str, query_end_ts_str: str):
        """Legacy elasticdump function (adapted for our system)"""
        
        timezone = index_config["es_timezone"]
        index_name = index_config["index_name"]
        index_host_url = index_config["index_host_url"]
        input_ca = index_config["ca_certs_path"]
        headers = index_config["header"]
        timestamp_field_name = index_config["timestamp_field_name"]
        aws_access_key = index_config["aws_access_key"]
        aws_secret_key = index_config["aws_secret_key"]
        
        # Build search body with custom filters
        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                timestamp_field_name: {
                                    "gte": query_start_ts_str,
                                    "lt": query_end_ts_str,
                                    "time_zone": 'PST8PDT'
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Add custom filters from config
        if self.config.get('must_exist'):
            for field in self.config.get('must_exist', []):
                search_body["query"]["bool"]["must"].append({"exists": {"field": field}})
        
        if self.config.get('exclude_null'):
            for field in self.config.get('exclude_null', []):
                search_body["query"]["bool"]["must_not"] = search_body["query"]["bool"].get("must_not", [])
                search_body["query"]["bool"]["must_not"].append({"term": {field: None}})
        
        # Get elasticdump options from config
        file_size = self.config.get('file_size_mb', 250)
        limit = self.config.get('elasticdump_limit', 10000)
        timeout = self.config.get('elasticdump_timeout', 21600)
        
        bash_command = [
            "elasticdump",
            f"--input={index_host_url}",
            f"--output={output_s3_path}",