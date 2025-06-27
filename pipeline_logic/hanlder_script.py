# handlers/config_handler.py
import json
import os
from pathlib import Path
from typing import Dict, Any

class ConfigHandler:
    def __init__(self, root_project_path: str):
        self.root_project_path = Path(root_project_path).resolve()  # Convert to absolute path
        self.airflow_vars = {}
        self.drive_defaults = {}
        self.index_config = {}
        self.host_name = None
        
    def load_index_config(self, index_config_relative_path: str) -> Dict[str, Any]:
        """Load index-specific configuration using absolute path"""
        # Convert relative path to absolute
        index_config_absolute_path = self.root_project_path / index_config_relative_path
        
        if not index_config_absolute_path.exists():
            raise FileNotFoundError(f"Index config not found: {index_config_absolute_path}")
            
        with open(index_config_absolute_path, 'r') as f:
            self.index_config = json.load(f)
            
        # Extract host_name from index config
        self.host_name = self.index_config.get("host_name")
        if not self.host_name:
            raise ValueError("host_name not found in index config")
        
        return self.index_config
    
    def load_drive_defaults(self, drive_defaults_relative_path: str) -> Dict[str, Any]:
        """Load drive table defaults using absolute path"""
        # Convert relative path to absolute
        drive_defaults_absolute_path = self.root_project_path / drive_defaults_relative_path
        
        if not drive_defaults_absolute_path.exists():
            raise FileNotFoundError(f"Drive defaults not found: {drive_defaults_absolute_path}")
            
        with open(drive_defaults_absolute_path, 'r') as f:
            drive_defaults_raw = json.load(f)
            
        # Extract the flattened defaults
        self.drive_defaults = drive_defaults_raw.get("default_drive_table_values", {})
        if not self.drive_defaults:
            raise ValueError("default_drive_table_values not found in drive defaults file")
            
        return self.drive_defaults
        
    def load_airflow_variables(self) -> Dict[str, Any]:
        """Load variables from Airflow using host_name from index config"""
        if not self.host_name:
            raise ValueError("host_name not found. Load index config first.")
            
        try:
            from airflow.models import Variable
            
            env = Variable.get("env", default_var="dev")
            
            # Get environment-specific variables using extracted host_name
            airflow_vars = {
                "env": env,
                
                # Elasticsearch credentials using host_name from config
                "es_username": Variable.get(f"{self.host_name}_user"),
                "es_password": Variable.get(f"{self.host_name}_password"),
                "es_ca_certs_path": Variable.get(f"{self.host_name}_ca_certs_path"),
                "es_header": Variable.get(f"{self.host_name}_header"),
                
                # AWS S3 config
                "aws_access_key_id": Variable.get(f"{env}_s3_access_key_id"),
                "aws_secret_access_key": Variable.get(f"{env}_s3_secret_access_key"),
                "s3_region": Variable.get(f"{env}_s3_region"),
                "s3_bucket_name": Variable.get(f"{env}_s3_bucket_name"),
                
                # Snowflake credentials
                "sf_username": Variable.get(f"{env}_snowflake_user"),
                "sf_password": Variable.get(f"{env}_snowflake_password"),
                "sf_account": Variable.get(f"{env}_snowflake_account"),
                "sf_warehouse": Variable.get(f"{env}_snowflake_warehouse"),
                "sf_role": Variable.get(f"{env}_snowflake_role")
            }
            
            print(f"✅ Successfully loaded Airflow variables for host: {self.host_name}, env: {env}")
            
        except (ModuleNotFoundError, Exception) as e:
            print(f"⚠️  Airflow not available ({e}), using dev fallback credentials")
            
            # Complete fallback credentials for local development
            airflow_vars = {
                "env": "dev",
                
                # Elasticsearch dev credentials
                "es_username": "dev_es_user",
                "es_password": "dev_es_password", 
                "es_ca_certs_path": "/path/to/dev/ca_certs.pem",
                "es_header": '{"Authorization": "Basic ZGV2X3VzZXI6ZGV2X3Bhc3M="}',
                
                # AWS S3 dev credentials
                "aws_access_key_id": "AKIA_DEV_ACCESS_KEY",
                "aws_secret_access_key": "dev_secret_access_key_12345",
                "s3_region": "us-west-1",
                "s3_bucket_name": "dev-test-data-bucket",
                
                # Snowflake dev credentials  
                "sf_username": "dev_sf_user",
                "sf_password": "dev_sf_password",
                "sf_account": "dev_account.us-west-1",
                "sf_warehouse": "DEV_COMPUTE_WH",
                "sf_role": "DEV_DATA_LOADER_ROLE"
            }
            
        self.airflow_vars = airflow_vars
        return airflow_vars
    
    def merge_configs(self) -> Dict[str, Any]:
        """Merge all configs with priority: Airflow > Index > Drive Defaults"""
        
        # Start with flattened drive defaults
        merged_config = self.drive_defaults.copy()
        
        # Override with index config values (all flattened)
        merged_config.update(self.index_config)
        
        # Override with airflow variables (highest priority)
        merged_config.update(self._map_airflow_to_config())
        
        # Replace placeholders
        merged_config = self._replace_placeholders(merged_config)
        
        return merged_config
    
    def _map_airflow_to_config(self) -> Dict[str, Any]:
        """Map airflow variable names to config keys"""
        mapping = {}
        
        # S3 mappings
        if self.airflow_vars.get("s3_bucket_name"):
            mapping["bucket_name"] = self.airflow_vars["s3_bucket_name"]
        if self.airflow_vars.get("s3_region"):
            mapping["s3_region"] = self.airflow_vars["s3_region"]
            
        # Elasticsearch mappings
        if self.airflow_vars.get("es_username"):
            mapping["es_username"] = self.airflow_vars["es_username"]
        if self.airflow_vars.get("es_password"):
            mapping["es_password"] = self.airflow_vars["es_password"]
        if self.airflow_vars.get("es_ca_certs_path"):
            mapping["es_ca_certs_path"] = self.airflow_vars["es_ca_certs_path"]
        if self.airflow_vars.get("es_header"):
            mapping["es_header"] = self.airflow_vars["es_header"]
            
        # Snowflake mappings
        if self.airflow_vars.get("sf_username"):
            mapping["sf_username"] = self.airflow_vars["sf_username"]
        if self.airflow_vars.get("sf_password"):
            mapping["sf_password"] = self.airflow_vars["sf_password"]
        if self.airflow_vars.get("sf_account"):
            mapping["sf_account"] = self.airflow_vars["sf_account"]
        if self.airflow_vars.get("sf_warehouse"):
            mapping["sf_warehouse"] = self.airflow_vars["sf_warehouse"]
        if self.airflow_vars.get("sf_role"):
            mapping["sf_role"] = self.airflow_vars["sf_role"]
            
        # AWS credentials
        if self.airflow_vars.get("aws_access_key_id"):
            mapping["aws_access_key_id"] = self.airflow_vars["aws_access_key_id"]
        if self.airflow_vars.get("aws_secret_access_key"):
            mapping["aws_secret_access_key"] = self.airflow_vars["aws_secret_access_key"]
            
        return mapping
    
    def _replace_placeholders(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Replace {env}, {index_group}, {index_name} placeholders"""
        env = self.airflow_vars.get("env", "dev")
        index_group = config.get("index_group", "")
        index_name = config.get("index_name", "")
        
        def replace_value(value):
            if isinstance(value, str):
                return value.replace("{env}", env).replace("{index_group}", index_group).replace("{index_name}", index_name)
            elif isinstance(value, list):
                return [replace_value(item) for item in value]
            elif isinstance(value, dict):
                return {k: replace_value(v) for k, v in value.items()}
            return value
        
        # Apply placeholder replacement to all values
        replaced_config = {}
        for key, value in config.items():
            replaced_config[key] = replace_value(value)
            
        return replaced_config

# Usage function
def get_final_config(root_project_path: str, drive_defaults_relative_path: str, index_config_relative_path: str) -> Dict[str, Any]:
    """
    Get final merged configuration
    
    Args:
        root_project_path: Absolute path to main project folder
        drive_defaults_relative_path: Relative path from root to drive defaults (e.g., "pipeline_logic/config/drive_table_defaults.json")
        index_config_relative_path: Relative path from root to index config (e.g., "projects-using-this-logic/project1/application_logs/config.json")
    
    Returns:
        Dict with all 61+ columns ready for database insertion
    """
    handler = ConfigHandler(root_project_path)
    
    # Load configs in order (index config first to get host_name)
    handler.load_index_config(index_config_relative_path)
    handler.load_drive_defaults(drive_defaults_relative_path)
    handler.load_airflow_variables()  # Uses host_name from index config
    
    # Merge with priority: Airflow > Index > Drive Defaults
    final_config = handler.merge_configs()
    
    print(f" Config merged successfully. Total keys: {len(final_config)}")
    
    return final_config


# if __name__ == "__main__":
#     config = get_final_config(
#         root_project_path="/home/user/my_pipeline_project",
#         drive_defaults_relative_path="pipeline_logic/config/drive_table_defaults.json",
#         index_config_relative_path="projects-using-this-logic/project1/application_logs/config.json"
#     )
    
#     print("Sample config keys:", list(config.keys())[:10])
#     print("Source name:", config.get("source_name"))
#     print("Environment:", config.get("env"))