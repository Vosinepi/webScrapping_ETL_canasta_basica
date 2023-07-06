import datetime as dt
from datetime import datetime, timedelta
import tweepy
import pandas as pd
from airflow.models import Variable

# from airflow import DAG
# from airflow.decorators import task, dag

# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.email_operator import EmailOperator

# Credenciales de Twitter
from certificados_twitter import api_key, api_secret_key, access_token, secret_token

# fechas variables
fecha_actual = dt.datetime.now()
fecha = dt.datetime.now().strftime("%Y-%m-%d")

print(fecha)
primer_dia_mes_actual = dt.datetime.now().replace(day=1).strftime("%Y-%m-%d")


# fin de mes

primer_dia_siguiente_mes = dt.date(fecha_actual.year, fecha_actual.month + 1, 1)
print(primer_dia_siguiente_mes)
ultimo_dia_mes_actual = primer_dia_siguiente_mes - dt.timedelta(days=1)
print(ultimo_dia_mes_actual)
# Verificar si el día actual es el último día del mes
es_fin_de_mes = fecha == ultimo_dia_mes_actual.strftime("%Y-%m-%d")
print(fecha)
print(es_fin_de_mes)

# primero de mes
primer_dia = fecha == primer_dia_mes_actual
print(primer_dia)


# nombre del mes en español


meses = {
    "January": "Enero",
    "February": "Febrero",
    "March": "Marzo",
    "April": "Abril",
    "May": "Mayo",
    "June": "Junio",
    "July": "Julio",
    "August": "Agosto",
    "September": "Septiembre",
    "October": "Octubre",
    "November": "Noviembre",
    "December": "Diciembre",
}

nombre_mes = meses[dt.datetime.now().strftime("%B")]
print(nombre_mes)

# cargamos el csv de precios
try:
    lista_larga = pd.read_csv("/opt/airflow/data/precios_lista_larga.csv")
except:
    lista_larga = pd.read_csv(
        r"C:\codeo_juego\scrapping_coto_canasta_basica\airflow\data\precios_lista_larga.csv"
    )


# precios del primer dia del mes actual y del dia actual
def precios(lista):
    # Precios del primer día del mes actual
    if fecha == lista["fecha"].max():
        precios_primer_dia_mes_actual = lista_larga[
            lista_larga["fecha"] == primer_dia_mes_actual
        ]

        # precios del dia actual
        precios_dia_actual = lista_larga[lista_larga["fecha"] == fecha]
        return precios_primer_dia_mes_actual, precios_dia_actual

    else:
        print("No hay precios del dia actual")
        return False, False


# limpio los precios
def limpio_precios(primer_dia_mes, fecha_actual):
    if primer_dia_mes is False or fecha_actual is False:
        print("No hay precios del dia actual")
        return False, False
    else:
        mask1 = fecha_actual["precio"] == 0
        mask2 = fecha_actual.groupby("producto")["precio"].transform("first") == 0
        mask3 = primer_dia_mes["precio"] == 0
        mask4 = primer_dia_mes.groupby("producto")["precio"].transform("first") == 0

        # Eliminar los productos sin precio
        primer_dia_mes = primer_dia_mes[primer_dia_mes["precio"] != 0]
        primer_dia_mes = primer_dia_mes[
            primer_dia_mes["producto"].isin(fecha_actual.loc[~mask2, "producto"])
        ]

        fecha_actual = fecha_actual[fecha_actual["precio"] != 0]
        fecha_actual = fecha_actual[
            fecha_actual["producto"].isin(primer_dia_mes.loc[~mask4, "producto"])
        ]

        # Restablecer los índices
        primer_dia_mes.reset_index(drop=True, inplace=True)
        fecha_actual.reset_index(drop=True, inplace=True)

        return primer_dia_mes, fecha_actual


# saco la variacion de precios mensual al dia de la fecha
def variacion_precios(precios_limpios):
    if precios_limpios[0] is False or precios_limpios[1] is False:
        print("No hay precios del dia actual")
        return False
    else:
        precio_anterior = sum(precios_limpios[0]["precio"])
        precio_actual = sum(precios_limpios[1]["precio"])

        # calculo la variación

        variacion = round((precio_actual / precio_anterior - 1) * 100, 2)
        print(f"La variacion es {variacion}\n")
        return variacion


variacion = variacion_precios(
    limpio_precios(precios(lista_larga)[0], precios(lista_larga)[1])
)


# obtengo los 4 productos con mas variacion de precios
def productos_mas_variacion(limpio_precios):
    precios_primer_dia_mes_actual = limpio_precios[0]

    precios_dia_actual = limpio_precios[1]

    # calculo la variacion de precios
    precios_primer_dia_mes_actual["variacion"] = (
        precios_dia_actual["precio"] / precios_primer_dia_mes_actual["precio"] - 1
    ) * 100

    # ordeno los productos por variacion
    precios_primer_dia_mes_actual.sort_values(
        by=["variacion"], ascending=False, inplace=True
    )

    # me quedo con los 4 productos con mas variacion
    productos_mas_variacion = precios_primer_dia_mes_actual.head(4)
    productos_menor_variacion = precios_primer_dia_mes_actual.tail(4)

    # armo diccionarios con los productos y sus variaciones
    mas_variacion = dict(
        zip(
            productos_mas_variacion["producto"],
            round(productos_mas_variacion["variacion"], 2),
        )
    )
    menor_variacion = dict(
        zip(
            productos_menor_variacion["producto"],
            round(productos_menor_variacion["variacion"], 2),
        )
    )

    return mas_variacion, menor_variacion


# productos_mas_variacion(
#     limpio_precios(precios(lista_larga)[0], precios(lista_larga)[1])
# )


# creo el mensaje de twiter
def mensaje_twitter(variacion):
    # obtengo los productos con mas y menor variacion
    no_disponible = ""

    try:
        productos_con_variaciones = productos_mas_variacion(
            limpio_precios(precios(lista_larga)[0], precios(lista_larga)[1])
        )
        max = productos_con_variaciones[0]
        min = productos_con_variaciones[1]

        # productos con precio cero o no disponibles
        productos = precios(lista_larga)[1]
        if isinstance(productos, pd.DataFrame):
            productos_no_disponibles = productos.loc[
                productos["precio"] == 0, "producto"
            ].tolist()
            no_disponible = ", ".join(productos_no_disponibles)
            print(f"Los productos sin precio son: {no_disponible}")
        else:
            productos_no_disponibles = []
            no_disponible = f"No hay productos sin precios el dia {dt.datetime.now().day} de {nombre_mes}"
            print(no_disponible)

        # saco los guines bajos de los nombres de los productos
        max = {k.replace("_", " "): v for k, v in max.items()}
        min = {k.replace("_", " "): v for k, v in min.items()}
        print(max)
        print(min)
    except:
        max = None
        min = None

    # si no hay precios del dia actual
    if precios(lista_larga)[0] is False:
        mensaje = f"No hay precios del dia {dt.datetime.now().day} de {nombre_mes}"
        mensaje_max = None
        mensaje_min = None

        return mensaje, mensaje_max, mensaje_min, no_disponible

    # si es fin de mes y hay precios del dia actual
    elif es_fin_de_mes:
        mensaje = f"La variación de precios de la canasta básica en el mes de {nombre_mes} es del {variacion}%"
        mensaje_max = f"Los productos con mayor variación del mes de {nombre_mes} son: "
        mensaje_max += (
            ", ".join(
                [
                    f"\n{variacion:.2f}% para {producto}"
                    for producto, variacion in max.items()
                ]
            )
            if max is not None
            else mensaje_max + " No items found."
        )
        mensaje_min = (
            f"Los productos que más redujeron su precio en el mes de {nombre_mes} son: "
        )
        mensaje_min += (
            ", ".join(
                [
                    f"\n{variacion:.2f}% para {producto}"
                    for producto, variacion in min.items()
                ]
            )
            if min is not None
            else mensaje_min + " No items found."
        )

        return mensaje, mensaje_max, mensaje_min, no_disponible

    # si no es fin de mes y hay precios del dia actual
    else:
        mensaje = f"La variación de precios de la canasta básica en el mes de {nombre_mes} al día {dt.datetime.now().day} es del {variacion}%"
        mensaje_max = f"Los productos con mayor aumento de {nombre_mes} al día de hoy {dt.datetime.now().day} son:"
        mensaje_max += (
            ", ".join(
                [
                    f"\n{variacion:.2f}% para {producto}"
                    for producto, variacion in max.items()
                ]
            )
            if max is not None
            else mensaje_max + " No items found."
        )
        mensaje_min = f"Los productos con mayor reducción de precio en el mes de {nombre_mes} al día de hoy {dt.datetime.now().day} son:"
        mensaje_min += (
            ", ".join(
                [
                    f"\n{variacion:.2f}% para {producto}"
                    for producto, variacion in min.items()
                ]
            )
            if min is not None
            else mensaje_min + " No items found."
        )

        return mensaje, mensaje_max, mensaje_min, no_disponible


# twitteo la variacion de precios
def twitear(mensaje):
    mensajes = mensaje
    # Variable.set("enviado", True)
    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret_key,
        access_token=access_token,
        access_token_secret=secret_token,
    )
    try:
        if precios(lista_larga)[0] is False:
            client.create_tweet(text=mensajes[0])
            print(mensajes[0])
        elif primer_dia:
            client.create_tweet(text="Mes nuevo, precios nuevos!, a cruzar los dedos 🤞")
            print("Mes nuevo, precios nuevos!, a cruzar los dedos 🤞")

        else:
            client.create_tweet(text=mensajes[0])
            client.create_tweet(text=mensajes[1])
            client.create_tweet(text=mensajes[2])

            print(mensajes[0])
            print(mensajes[1])
            print(mensajes[2])
    except:
        print("Ya twitee hoy")


# twiteo con tweepy

# twitear(mensaje_twitter(variacion))

# dag = DAG(
#     dag_id="02_bot_twitter_canasta_basica",
#     description="Bot Twitter Canasta Básica",
#     schedule_interval="30 12 * * *",
#     default_args={
#         "owner": "airflow",
#         "retries": 1,
#         "retry_delay": timedelta(minutes=20),
#         "start_date": datetime(2023, 3, 16),
#         "email": ["ismaelpiovani@gmail.com"],
#         "email_on_failure": True,
#         "email_on_retry": True,
#     },
#     catchup=False,
# )

# t0 = PythonOperator(
#     task_id="productos_mas_variacion",
#     python_callable=productos_mas_variacion,
#     op_kwargs={"limpio_precios": limpio_precios},
#     dag=dag,
# )

# t1 = PythonOperator(
#     task_id="mensaje_twitter",
#     python_callable=mensaje_twitter,
#     op_kwargs={"variacion": variacion, "max": max, "min": min},
#     dag=dag,
# )

# t2 = PythonOperator(
#     task_id="twitear",
#     python_callable=twitear,
#     op_kwargs={
#         "texto1": mensaje_twitter(variacion, max, min)[0],
#         "texto2": mensaje_twitter(variacion, max, min)[1],
#         "texto3": mensaje_twitter(variacion, max, min)[2],
#     },
#     dag=dag,
# )

# t3 = EmailOperator(
#     task_id="email",
#     to="ismaelpiovani@gmail.com",
#     subject="Bot_twitter_canasta_basica",
#     html_content=f"""
#     <h3>La variación de precios de la canasta básica en el mes de {nombre_mes} al día {dt.datetime.now().day} es del {variacion}%</h3>
#     <h4>Los precios con mayor aumento al día de hoy son:</h4>
#     <p>{mensaje_twitter(variacion, max, min)[1]}</p>
#     <h4>Los precios con mayor reducción de precio al día de hoy son:</h4>
#     <p>{mensaje_twitter(variacion, max, min)[2]}</p>
#     """,
#     dag=dag,
# )
