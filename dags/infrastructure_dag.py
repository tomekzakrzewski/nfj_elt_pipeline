from datetime import datetime, timedelta
from airflow.decorators import dag, task
from config.config import GCS_CONFIG
from src.scraping.nfj_scrape import scrape_json
from src.utils.gcs_utils import create_gcs_bucket
import logging


default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id='infrastructure_setup',
    description='Initial set up of GCP bucket',
    default_args=default_args,
    start_date=datetime(2024, 11, 5),
    schedule_interval='@once'
)
def setup_infrastructure():

    @task(task_id='create_gcs_bucket')
    def create_bucket():
        logging.info("Creating storage bucket...")
        create_gcs_bucket(
            bucket_name=GCS_CONFIG["raw_bucket"],
            storage_class=GCS_CONFIG["storage"],
            location=GCS_CONFIG["location"]
        ).name
        logging.info("Storage bucket created")

    bucket_name = create_bucket()

infra_dag = setup_infrastructure()
