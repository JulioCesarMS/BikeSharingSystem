from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import run
import sys
import os

# Agregar path del proyecto
sys.path.append('/app')

from main import run

default_args = {
    'owner': 'julio',
    'retries': 2
}

with DAG(
    dag_id='ecobicis_etl',
    default_args=default_args,
    schedule_interval='0 2 1 * *',  # mensual
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=run
    )