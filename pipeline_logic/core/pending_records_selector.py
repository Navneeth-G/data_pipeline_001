# pipeline_logic/core/pending_records_selector.py
from typing import Dict, Any
import pandas as pd
from pipeline_logic.utils.log_generator import log

class PendingRecordsSelector:
    def __init__(self, final_config: Dict[str, Any], sf_client):
        self.config = final_config
        self.sf_client = sf_client
        
    def get_pending_records(self, limit_count: int) -> pd.DataFrame:
        """Get N pending records sorted by priority criteria"""
        query = f"""
        SELECT *
        FROM {self.config.get('drive_table')}
        WHERE source_name = %(source_name)s
          AND source_category = %(source_category)s  
          AND source_subcategory = %(source_subcategory)s
          AND pipeline_status = 'pending'
        ORDER BY source_query_window_start_time ASC, 
                 pipeline_retry_count DESC, 
                 pipeline_priority ASC
        LIMIT %(limit_count)s
        """
        
        params = {
            'source_name': self.config.get('source_name'),
            'source_category': self.config.get('source_category'),
            'source_subcategory': self.config.get('source_subcategory'),
            'limit_count': limit_count
        }
        
        result = self.sf_client.fetch_all_rows_as_dataframe(query, params)
        pending_df = result.get('data', pd.DataFrame())
        
        log.info(
            f"Retrieved {len(pending_df)} pending records",
            log_key="Pending Records Selection",
            status="SUCCESS",
            limit_requested=limit_count,
            records_found=len(pending_df)
        )
        
        return pending_df




