from config.config import GCP_CONFIG
import pandas as pd

def load_raw_data_to_bigquery(df: pd.DataFrame, table_name: str, project_id: str):
    df.to_gbq(
        destination_table=f"staging.stg_{table_name}",
        project_id=project_id,
        if_exists='append',
        location= GCP_CONFIG['location']
    )
