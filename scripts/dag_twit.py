import datetime as dt
import tweepy
import pandas as pd


from bot_twit import (
    variacion,
    productos_mas_variacion,
    twitear,
    mensaje_twitter,
    limpio_precios,
    precios,
    lista_larga,
    nombre_mes,
)

dag = DAG(
    dag_id="03_dag_twit",
    start_date=dt.datetime.now(),
    schedule_interval="45 11 * * *",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=5),
    },
)


t1 = PythonOperator(
    task_id="productos_variacion",
    python_callable=productos_mas_variacion,
    op_kwargs={
        "limpio_precios": limpio_precios(
            precios(lista_larga)[0], precios(lista_larga)[1]
        )
    },
    dag=dag,
)

t2 = PythonOperator(
    task_id="twitear",
    python_callable=twitear,
    op_kwargs={
        "texto1": mensaje_twitter(variacion, max, min)[0],
        "texto2": mensaje_twitter(variacion, max, min)[1],
        "texto3": mensaje_twitter(variacion, max, min)[2],
    },
    dag=dag,
)

t3 = EmailOperator(
    task_id="email",
    to="ismaelpiovani@gmail.com",
    subject="Bot_twitter_canasta_basica",
    html_content=f"""
    <h3>La variación de precios de la canasta básica en el mes de {nombre_mes} al día {dt.datetime.now().day} es del {variacion}%</h3>
    <h4>Los productos con mayor aumento al día de hoy son:</h4>
    <p>{mensaje_twitter(variacion, max, min)[1]}</p>
    <h4>Los productos con mayor reducción de precio al día de hoy son:</h4>
    <p>{mensaje_twitter(variacion, max, min)[2]}</p>
    """,
    dag=dag,
)


t1 >> t2 >> t3
