from datetime import datetime, timedelta
from airflow.decorators import dag, task
from config.config import GCS_CONFIG
from src.scraping.nfj_scrape import scrape_json
from src.utils.gcs_utils import create_gcs_bucket, check_if_bucket_exists
from src.loaders.gcs_loader import upload_to_gcs
from airflow.exceptions import AirflowException
import logging

PAGESIZE_INITIAL = 1500
PAGESIZE_DAILY = 50
default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id='scrape_raw_ingest',
    description='Initial scrape and raw data ingestion',
    default_args=default_args,
    start_date=datetime(2024, 11, 5),
    schedule_interval='@daily',
    catchup=False,
)
def initial_scrape_and_raw_data_load():
    @task
    def check_bucket_exists():
        bucket_name = GCS_CONFIG['raw_bucket']
        try:
            logging.info(f"Checking if bucket {bucket_name} exists...")
            bucket_exists = check_if_bucket_exists(bucket_name)
            if bucket_exists:
                logging.info(f"Bucket {bucket_name} exists")
                return True
            else:
                logging.info(f"Bucket {bucket_name} does not exists")
                raise AirflowException(f"Bucket {bucket_name} does not exist")
        except Exception as e:
            logging.exception(f"Error checking bucket '{bucket_name}': {str(e)}")
            raise

    @task
    def determine_pagesize(**kwargs):
        execution_date = kwargs['execution_date']
        dag_start_date = kwargs['dag'].start_date

        if execution_date == dag_start_date:
            pagesize = PAGESIZE_INITIAL
        else:
            pagesize = PAGESIZE_DAILY

        logging.info(f"Pagesize set to {pagesize}")
        return pagesize

    @task(task_id='scrape_all_jobs')
    def scrape_jobs(**kwargs):
        pagesize = kwargs['ti'].xcom_pull(task_ids='determine_pagesize')
        logging.info(f"Starting scraping with pagesize: {pagesize}...")
        try:
            data = scrape_json(pagesize)
            logging.info(f"Successfully scraped job offers")
            return data
        except Exception as e:
            logging.exception(f"Error during job scraping.")
            raise

    @task(task_id='raw_data_ingestion_to_gcs')
    def upload_to_bucket(data):
        bucket_name = GCS_CONFIG['raw_bucket']
        try:
            logging.info(f"Uploading data to bucket: {bucket_name}...")
            gcs_uri = upload_to_gcs(data, bucket_name)
            logging.info(f"Data successfully uploaded to {gcs_uri}")
            return gcs_uri
        except Exception as e:
            logging.exception("Error during data upload")
            raise

    bucket_exists = check_bucket_exists()
    pagesize = determine_pagesize()
    scraped_data = scrape_jobs(op_kwargs={'ti': pagesize})
    bucket_exists >> pagesize >> scraped_data

    data_uri = upload_to_bucket(scraped_data)
    scraped_data >> data_uri

scrape_and_ingest_dag = initial_scrape_and_raw_data_load()
