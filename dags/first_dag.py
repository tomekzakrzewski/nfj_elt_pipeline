from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.scraping.nfj_scrape import scrape_json
from src.utils.gcp_utils import create_gcs_bucket


default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

# tutaj scrape json bedzie zwracal wartosc
# potem nastepny dag bedzie wrzucal do bucketa

@dag(dag_id='initial_scrape_and_raw_ingest',
    default_args=default_args,
    start_date=datetime(2023, 5, 5),
    schedule_interval='@daily')

def initial_scrape_data_ingestion():

    # @task()
    # def initial_scrape_json():
    #     json_data = scrape_json(2)
    #     return json_data

    # @task()
    # def print_data(data):
    #     print(data)

    @task()
    def create_bucket():
        create_gcs_bucket()


    # json_data = initial_scrape_json()
    # print_data(data=json_data)
    create_bucket()



etl = initial_scrape_data_ingestion()
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
