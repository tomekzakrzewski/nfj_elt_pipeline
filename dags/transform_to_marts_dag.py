from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from airflow.decorators import dag
import logging

default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id='transform_marts',
    description='Transform staging data into marts using dbt',
    default_args=default_args,
    start_date=datetime(2024, 10, 29),
    schedule_interval='@daily',
    catchup=False,
)
def transform_to_marts():

    logging.info("Setting up dbt configurations for transforming marts.")
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

    transform_marts_task = DbtTaskGroup(
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
                'marts.fct_jobs',
                'marts.fct_job_requirements'
            ]
        }
    )
    logging.info("dbt transformation setup complete.")

    transform_marts_task

marts_dag = transform_to_marts()
