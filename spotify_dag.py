"""
Algorítimo baseado no vídeo e exemplo de Karolina Sowinska (YouTube - https://www.youtube.com/watch?v=i25ttd32-eo)
	- Crédito especial para ela.
"""


from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from spotify_etl import run_spotify_etl
from spotify_from_db_to_csv import run_from_db_to_csv

default_args = {
    "owner": "Diego G.",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 20),
    "email": [""],  # Insert email
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "spotify_dag",
    default_args=default_args,
    description="Extract previewsly played songs on spotify.",
    schedule_interval=timedelta(minutes=5),
)


def just_a_function():
    print("We're busy working here...")


run_etl = PythonOperator(
    task_id="whole_spotify_etl", python_callable=run_spotify_etl, dag=dag
)

run_from_db_to_csv = PythonOperator(
    task_id="from_db_to_csv", python_callable=run_from_db_to_csv, dag=dag
)

run_etl >> run_from_db_to_csv
