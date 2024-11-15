from datetime import datetime, timedelta
from airflow.decorators import dag, task
from config.config import GCS_CONFIG
from src.scraping.nfj_scrape import scrape_json
from src.utils.gcs_utils import create_gcs_bucket, check_if_bucket_exists
from src.loaders.gcs_loader import upload_to_gcs
from airflow.exceptions import AirflowException

PAGESIZE_INITIAL = 1500
PAGESIZE_DAILY = 50
default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id='scrape_raw_ingest',
    description='Initial scrape and raw data ingestion',
    default_args=default_args,
    start_date=datetime(2024, 11, 5),
    schedule_interval='@daily',
    catchup=False
)
def initial_scrape_and_raw_data_load():
    @task
    def check_bucket_exists():
        bucket_name = GCS_CONFIG['raw_bucket']
        try:
            bucket_exists = check_if_bucket_exists(bucket_name)
            if bucket_exists:
                return True
            else:
                raise AirflowException(f"Bucket {bucket_name} does not exist")
        except Exception as e:
            raise AirflowException(f"Error checking {bucket_name}: {str(e)}")

    @task
    def determine_pagesize(**kwargs):
        execution_date = kwargs['execution_date']
        dag_start_date = kwargs['dag'].start_date
        ## if first run, pagezise initial, else daily
        if execution_date == dag_start_date:
            return PAGESIZE_INITIAL
        else:
            return PAGESIZE_DAILY
        # return PAGESIZE_INITIAL if execution_date == dag_start_date else PAGESIZE_DAILY

    @task(task_id='scrape_all_jobs')
    def scrape_jobs(**kwargs):
        pagesize = kwargs['ti'].xcom_pull(task_ids='determine_pagesize')
        try:
            data = scrape_json(pagesize)
            return data
        except Exception as e:
            print("Something went wrong")
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

    bucket_exists = check_bucket_exists()
    pagesize = determine_pagesize()
    scraped_data = scrape_jobs(op_kwargs={'ti': pagesize})
    bucket_exists >> pagesize >> scraped_data

    data_uri = upload_to_bucket(scraped_data)
    scraped_data >> data_uri

scrape_and_ingest_dag = initial_scrape_and_raw_data_load()
