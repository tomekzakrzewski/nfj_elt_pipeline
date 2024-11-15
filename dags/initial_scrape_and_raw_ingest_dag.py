from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState
from config.config import GCS_CONFIG
from src.scraping.nfj_scrape import scrape_json
from src.utils.gcs_utils import create_gcs_bucket
from src.loaders.gcs_loader import upload_to_gcs

PAGESIZE = 100
default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id='scrape_raw_ingest',
    description='Initial scrape and raw data ingestion',
    default_args=default_args,
    start_date=datetime(2024, 11, 5),
    schedule_interval='@once'
)
def initial_scrape_and_raw_data_load():

    wait_for_infrastructure = ExternalTaskSensor(
        task_id='wait_for_infra',
        external_dag_id='infrastructure_setup',
        external_task_id='create_gc_bucket',
        mode='poke',
        allowed_states=['success']
    )

    @task(task_id='scrape_all_jobs')
    def scrape_all_jobs():
        try:
            data = scrape_json(PAGESIZE)
            return data
        except Exception as e:
            print("something went wrong")
            raise

    @task(task_id='raw_data_ingestion_to_gcs')
    def upload_to_bucket(data):
        bucket_name = GCS_CONFIG['raw_bucket']
        try:
            gcs_uri = upload_to_gcs(data, bucket_name)
            return gcs_uri
        except Exception as e:
            print("something went wrong while uploading")
            raise

    scraped_data = scrape_all_jobs()
    # wait_for_infrastructure >> scraped_data

    data_uri = upload_to_bucket(scraped_data)
    scraped_data >> data_uri

scrape_and_ingest_dag = initial_scrape_and_raw_data_load()
