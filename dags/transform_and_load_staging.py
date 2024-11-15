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
from airflow.operators.python import PythonOperator


default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id='transform_raw_and_load',
    description='transform raw data and load into staging big query',
    default_args=default_args,
    start_date=datetime(2024, 11, 5),
    schedule_interval='@once',
    catchup=False
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

        # dict for better handling xcomargs
        return {
            "jobs": df_jobs.to_dict(),
            "requirements": df_requirements.to_dict(),
            "jobs_requirements": df_jobs_and_requirements.to_dict()
        }

    @task
    def load_to_staging(transformed_data: dict) -> None:
        """Load transformed data to BigQuery staging tables"""
        project_id = GCP_CONFIG['project_id']

        # Convert dict back to dataframes
        df_jobs = pd.DataFrame.from_dict(transformed_data["jobs"])
        df_requirements = pd.DataFrame.from_dict(transformed_data["requirements"])
        df_jobs_requirements = pd.DataFrame.from_dict(transformed_data["jobs_requirements"])

        # Load to BigQuery
        load_raw_data_to_bigquery(df_jobs, "jobs", project_id)
        load_raw_data_to_bigquery(df_requirements, "requirements", project_id)
        load_raw_data_to_bigquery(df_jobs_requirements, "job_requirements", project_id)


    dbt_config = ProjectConfig(
        dbt_project_path="/opt/airflow/dbt_transforms",
        models_relative_path="models",
        project_name="dbt_transforms",
        env_vars={
            "DBT_PROFILES_DIR": "/opt/airflow/config",
            "DBT_PROJECT_DIR": "/opt/airflow/dbt_transforms",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/config/nfj-elt-project-key.json"
        }
    )

    profile_config = ProfileConfig(
        profile_name="dbt_transforms",
        target_name="dev",
        profiles_yml_filepath="/opt/airflow/config/profiles.yml"
    )

    transform_marts = DbtTaskGroup(
        group_id="transform_marts",
        project_config=dbt_config,
        profile_config=profile_config,
        operator_args={
            "select": [
                'marts.dim_companies',
                'marts.dim_categories',
                'marts.dim_seniority',
                'marts.dim_salary_ranges',
                'marts.dim_requirements',
                # Facts last
                'marts.fct_jobs',
                'marts.fct_job_requirements'
            ]
        }
    )
    # transform_marts = DbtTaskGroup(
    #     group_id="transform_marts",
    #     project_config=dbt_config,
    #     profile_config=profile_config,
    #     operator_args={
    #         "select": [
    #             # Dimensions first
    #             'marts.dim_companies',
    #             'marts.dim_categories',
    #             'marts.dim_seniority',
    #             'marts.dim_salary_ranges',
    #             'marts.dim_requirements',
    #             # Facts last
    #             'marts.fct_jobs',
    #             'marts.fct_job_requirements'
    #         ]
    #     }
    # )

    # Define task dependencies
    # Define task dependencies with type casting
    raw_data = get_latest_raw_data()
    transformed_data = transform_data(cast(dict[str, Any], raw_data))
    staging_load = load_to_staging(cast(dict[str, dict], transformed_data))

    chain(
        raw_data,
        transformed_data,
        staging_load,
        transform_marts
    )

# Create DAG instance
transform_load = transform_raw_load_staging()
