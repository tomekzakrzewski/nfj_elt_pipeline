from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.scraping.nfj_scrape import scrape_json


default_args = {"owner": "tomek", "retires": 5, "retry_delay": timedelta(minutes=2)}

# tutaj scrape json bedzie zwracal wartosc
# potem nastepny dag bedzie wrzucal do bucketa


with DAG(
    default_args=default_args,
    dag_id="scrape_dag",
    description="This is my first dag",
    start_date=datetime(2023, 11, 4),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="scrape_nfj_data",
        python_callable=scrape_json,
        op_kwargs={"pageSize": 5},
    )
