import requests as req
import pandas as pd

from bs4 import BeautifulSoup
import re
import psycopg2
import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def postgre_read():
    # Obtner data de la base de datos postgres e imprimirlo
    hook = PostgresHook(postgres_conn_id="Canasta")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM precios;")

    print(cursor.description)

    cursor.close()
    conn.close()


with DAG(
    dag_id="postres_prueba",
    description="DAG para scrapping de canasta familiar",
    schedule_interval="15 08 * * *",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=20),
        "start_date": datetime(2023, 3, 16),
        "email": ["ismaelpiovani@gmail.com"],
        "email_on_success": True,
        "email_on_failure": True,
        "email_on_retry": True,
    },
    catchup=False,
) as f:
    task1 = PythonOperator(task_id="prueba_postgres", python_callable=postgre_read)
