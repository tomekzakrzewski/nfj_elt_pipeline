from datetime import datetime, timedelta
from airflow.decorators import dag, task
from config.config import GCP_CONFIG
from src.scraping.nfj_scrape import scrape_json
from src.utils.gcp_utils import create_gcs_bucket


default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

# tutaj scrape json bedzie zwracal wartosc
# potem nastepny dag bedzie wrzucal do bucketa

@dag(
    dag_id='infrastructure_setup',
    default_args=default_args,
    start_date=datetime(2024, 11, 5),
    schedule_interval='@once'
)

def setup_infrastructure():

    # @task()
    # def initial_scrape_json():
    #     json_data = scrape_json(2)
    #     return json_data

    # @task()
    # def print_data(data):
    #     print(data)

    @task()
    def create_bucket():
        create_gcs_bucket(
            bucket_name=GCP_CONFIG["raw_bucket"],
            storage_class=GCP_CONFIG["storage"],
            location=GCP_CONFIG["location"]
        )


    # json_data = initial_scrape_json()
    # print_data(data=json_data)
    bucket = create_bucket()



dag = setup_infrastructure()
# with DAG(
#     default_args=default_args,
#     dag_id="scrape_dag",
#     description="This is my first dag",
#     start_date=datetime(2023, 11, 4),
#     schedule_interval="@daily",
# ) as dag:
#     task1 = PythonOperator(
#         task_id="scrape_nfj_data",
#         python_callable=scrape_json,
#         op_kwargs={"pageSize": 5},
#     )
