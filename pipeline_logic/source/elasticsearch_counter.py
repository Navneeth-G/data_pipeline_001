# pipeline_logic/source/elasticsearch_counter.py
import requests
import base64
from typing import Dict, Any
from pipeline_logic.utils.log_generator import log
from pipeline_logic.utils.time_utils import format_timestamp_for_elasticsearch

class ElasticsearchCounter:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
    
    def get_record_count(self, config: Dict[str, Any], start_time: str, end_time: str) -> int:
        """Get document count from Elasticsearch for source discovery"""
        return self._get_elasticsearch_count_by_time_window(start_time, end_time)
    
    def get_source_count_for_audit(self, config: Dict[str, Any], start_time: str, end_time: str) -> int:
        """Get document count from Elasticsearch for audit verification"""
        return self._get_elasticsearch_count_by_time_window(start_time, end_time)
    
    def _get_elasticsearch_count(self, unique_source_id: str) -> int:
        """Get document count from Elasticsearch for the time window"""
        
        # Get time window from drive table
        window_query = f"""
        SELECT source_query_window_start_time,
               source_query_window_end_time
        FROM {self.config.get('drive_table')}
        WHERE unique_source_id = %(unique_source_id)s
        """
        
        result = self.sf_client.fetch_all_rows_as_dataframe(window_query, {"unique_source_id": unique_source_id})
        df = result.get('data')
        batch_info = df.iloc[0]
        
        start_time = batch_info['source_query_window_start_time']
        end_time = batch_info['source_query_window_end_time']

        start_time = format_timestamp_for_elasticsearch(start_time)
        end_time = format_timestamp_for_elasticsearch(end_time)

        # Build Elasticsearch query
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                self.config.get('timestamp_field', '@timestamp'): {
                                    "gte": start_time,
                                    "lt": end_time
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Add custom filters if configured
        if self.config.get('must_exist'):
            for field in self.config.get('must_exist', []):
                es_query["query"]["bool"]["must"].append({"exists": {"field": field}})
        
        if self.config.get('exclude_null'):
            for field in self.config.get('exclude_null', []):
                es_query["query"]["bool"]["must_not"] = es_query["query"]["bool"].get("must_not", [])
                es_query["query"]["bool"]["must_not"].append({"term": {field: None}})
        
        # Make Elasticsearch count request
        es_url = f"http://{self.config.get('host_name')}:{self.config.get('port')}/{self.config.get('index_pattern')}/_count"
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Add authentication if configured
        if self.config.get('es_username') and self.config.get('es_password'):
            credentials = base64.b64encode(f"{self.config.get('es_username')}:{self.config.get('es_password')}".encode()).decode()
            headers['Authorization'] = f'Basic {credentials}'
        
        response = requests.post(es_url, headers=headers, json=es_query, timeout=60)
        response.raise_for_status()
        
        count_result = response.json()
        source_count = count_result.get('count', 0)
        
        log.info(
            f"Retrieved Elasticsearch count: {source_count}",
            log_key="Elasticsearch Count",
            status="SUCCESS",
            unique_source_id=unique_source_id,
            time_window=f"{start_time} to {end_time}",
            es_url=es_url
        )
        
        return source_count
    
    def _get_elasticsearch_count_by_time_window(self, start_time: str, end_time: str) -> int:
        """Get document count from Elasticsearch using direct time window"""
        
        start_time_formatted = format_timestamp_for_elasticsearch(start_time)
        end_time_formatted = format_timestamp_for_elasticsearch(end_time)

        # Build Elasticsearch query
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                self.config.get('timestamp_field', '@timestamp'): {
                                    "gte": start_time_formatted,
                                    "lt": end_time_formatted
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Add custom filters if configured
        if self.config.get('must_exist'):
            for field in self.config.get('must_exist', []):
                es_query["query"]["bool"]["must"].append({"exists": {"field": field}})
        
        if self.config.get('exclude_null'):
            for field in self.config.get('exclude_null', []):
                es_query["query"]["bool"]["must_not"] = es_query["query"]["bool"].get("must_not", [])
                es_query["query"]["bool"]["must_not"].append({"term": {field: None}})
        
        # Make Elasticsearch count request
        es_url = f"http://{self.config.get('host_name')}:{self.config.get('port')}/{self.config.get('index_pattern')}/_count"
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Add authentication if configured
        if self.config.get('es_username') and self.config.get('es_password'):
            credentials = base64.b64encode(f"{self.config.get('es_username')}:{self.config.get('es_password')}".encode()).decode()
            headers['Authorization'] = f'Basic {credentials}'
        
        response = requests.post(es_url, headers=headers, json=es_query, timeout=60)
        response.raise_for_status()
        
        count_result = response.json()
        source_count = count_result.get('count', 0)
        
        log.info(
            f"Retrieved Elasticsearch count: {source_count}",
            log_key="Elasticsearch Count",
            status="SUCCESS",
            time_window=f"{start_time_formatted} to {end_time_formatted}",
            es_url=es_url
        )
        
        return source_count

# Standalone functions for import compatibility
def get_record_count(config: Dict[str, Any], start_time: str, end_time: str) -> int:
    """Standalone function for source discovery"""
    # Create a temporary counter instance
    counter = ElasticsearchCounter(config, None)  # sf_client not needed for time-based counting
    return counter._get_elasticsearch_count_by_time_window(start_time, end_time)

def get_source_count_for_audit(config: Dict[str, Any], start_time: str, end_time: str) -> int:
    """Standalone function for audit verification"""
    # Create a temporary counter instance
    counter = ElasticsearchCounter(config, None)  # sf_client not needed for time-based counting
    return counter._get_elasticsearch_count_by_time_window(start_time, end_time)


