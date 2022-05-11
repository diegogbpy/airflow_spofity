"""
Tratamento de arquivo CSV conforme comando de atividade da cadeira de Eng. de Dados
	- remover colunas e selecionar valores específicos no arquivo
	- mover o arquivo para a pasta destino após o tratamento
"""

import datetime as dt
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {'owner': 'diego_gules',
                'start_date': dt.datetime(2021, 10, 21),
                'retries': 1,
                'retry_delay': dt.timedelta(minutes=5)}


def clean_telecom():
    df = pd.read_csv('/home/debian/Downloads/telecom_users.csv')
    df.drop(columns=['gender', 'SeniorCitizen', 'Partner', 'Dependents', 'tenure', 'PhoneService', 'MultipleLines',
                     'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV',
                     'StreamingMovies', 'Contract', 'PaperlessBilling', 'PaymentMethod', 'Churn',
                     ' ', ' .1', ' .2'], inplace=True)
    df = df[(df['InternetService'] == 'DSL')]
    df.to_csv('/home/debian/Downloads/telecom_users_cleaned.csv')


with DAG('CleanTelecom', default_args=default_args, schedule_interval=timedelta(minutes=5)) as dag:
    cleanTelecom = PythonOperator(task_id='clean', python_callable=clean_telecom)

    moveFile = BashOperator(task_id='move', bash_command='mv /home/debian/Downloads/telecom_users_cleaned.csv /tmp/.')

    cleanTelecom >> moveFile

