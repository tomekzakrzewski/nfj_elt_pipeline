from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from config.config import GCP_CONFIG
from config.config import GCS_CONFIG
from src.utils.gcs_utils import get_latest_blob
from src.transformers.job_transformer import transform_raw_data
from google.cloud.storage import Blob
from src.loaders.bigquery_loader import load_raw_data_to_bigquery
import json
import pandas as pd
from typing import cast, Any


default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id='transform_raw_and_load',
    description='transform raw data and load into staging big query',
    default_args=default_args,
    start_date=datetime(2024, 10, 29),
    schedule_interval='@daily',
    catchup=False,
)

def transform_raw_load_staging():

    @task
    def get_latest_raw_data() -> dict:
        bucket_name = GCS_CONFIG['raw_bucket']
        blob = get_latest_blob(bucket_name)

        content = blob.download_as_text()
        data = json.loads(content)

        return data

    @task(multiple_outputs=False)
    def transform_data(raw_data: dict) -> dict[str, dict]:
        df_jobs, df_requirements, df_jobs_and_requirements = transform_raw_data(raw_data)

        return {
            "jobs": df_jobs.to_dict(),
            "requirements": df_requirements.to_dict(),
            "jobs_requirements": df_jobs_and_requirements.to_dict()
        }

    @task
    def load_to_staging(transformed_data: dict) -> None:
        """Load transformed data to BigQuery staging tables"""
        project_id = GCP_CONFIG['project_id']

        df_jobs = pd.DataFrame.from_dict(transformed_data["jobs"])
        df_requirements = pd.DataFrame.from_dict(transformed_data["requirements"])
        df_jobs_requirements = pd.DataFrame.from_dict(transformed_data["jobs_requirements"])

        load_raw_data_to_bigquery(df_jobs, "jobs", project_id)
        load_raw_data_to_bigquery(df_requirements, "requirements", project_id)
        load_raw_data_to_bigquery(df_jobs_requirements, "job_requirements", project_id)

    raw_data = get_latest_raw_data()
    transformed_data = transform_data(cast(dict[str, Any], raw_data))
    staging_load = load_to_staging(cast(dict[str, dict], transformed_data))

    raw_data >> transformed_data >> staging_load

transform_load = transform_raw_load_staging()
