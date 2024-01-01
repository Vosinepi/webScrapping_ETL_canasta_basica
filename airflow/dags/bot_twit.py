import datetime as dt
from datetime import datetime, timedelta
import tweepy
import pandas as pd
import sys
import os
from airflow.models import Variable

# from airflow import DAG
# from airflow.decorators import task, dag

# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.email_operator import EmailOperator

# Agregar la carpeta 'plugins' al PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "plugins")))

# Credenciales de Twitter
from certificados_twitter import api_key, api_secret_key, access_token, secret_token
from variacion_perso import (
    variacion_personalizada,
    lista_variacion,
    precios,
    lista_larga,
)

from fechas import (
    fecha_actual,
    fecha,
    primer_dia_mes_actual,
    primer_dia_siguiente_mes,
    ultimo_dia_mes_actual,
    es_fin_de_mes,
    primer_dia,
    nombre_mes,
    semana_del_año,
    semana_pasada,
    primer_dia_semana_actual,
    primer_dia_semana_pasada,
    ultimo_dia_semana_pasada,
)

# fechas variables
primer_dia_semana = primer_dia_semana_actual.strftime(
    "%Y-%m-%d"
) == dt.datetime.now().strftime("%Y-%m-%d")

# fin de mes
print(primer_dia_siguiente_mes)

print(ultimo_dia_mes_actual)

# Verificar si el día actual es el último día del mes
print(es_fin_de_mes)

# primero de mes
print(primer_dia)
print(nombre_mes)


# chequeo si ya se envio twit hoy
def check_execution_status(fecha):
    print(fecha)
    """
    La función comprueba si una tarea ya se ha ejecutado hoy y devuelve True si puede continuar
    ejecutándose o False si debe interrumpirse.
    :return: un valor booleano. Si la tarea ya se ejecutó hoy, devuelve False. De lo contrario, devuelve
    Verdadero.
    """
    twit_executed_today = Variable.get("twit_executed_today", default_var=None)

    if twit_executed_today == fecha:
        return False  # La tarea ya se ha ejecutado hoy, se lanza la excepción.

    return True  # La tarea puede continuar ejecutándose.


# creo el mensaje de twiter
def mensaje_twitter(lista_cantidad, dia1, dia2):
    """
    La función `mensaje_twitter` genera un mensaje de Twitter con información sobre las variaciones de
    precio en un período de tiempo determinado para una lista de productos.

    :param lista_cantidad: Define cuantos productos se van a imprimir en el mensaje de Twitter.
    :param dia1: El parámetro "dia1" representa el primer día para el cual se desea calcular la
    variación de precios. Debe ser una fecha específica en el formato "AAAA-MM-DD"
    :param dia2: El parámetro "dia2" representa el segundo día para el cual se desea calcular la
    variación de precios. Se utiliza en la función "mensaje_twitter" para calcular la variación entre
    "dia1" y "dia2" y generar los mensajes correspondientes
    :return: The function `mensaje_twitter` returns four values: `mensaje`, `mensaje_max`,
    `mensaje_min`, and `no_disponible`.
    """
    variacion = variacion_personalizada(dia1, dia2)
    lista = lista_variacion(dia1, dia2, lista_cantidad)

    # obtengo los productos con mas y menor variacion
    no_disponible = ""

    try:
        max = lista[0]
        min = lista[1]

        # productos con precio cero o no disponibles
        productos = precios(lista_larga, dia1, dia2)[1]
        if isinstance(productos, pd.DataFrame):
            productos_no_disponibles = productos.loc[
                productos["precio"] == 0, "producto"
            ].tolist()
            no_disponible = ", ".join(productos_no_disponibles)
            print(f"Los productos sin precio son: {no_disponible}")
        else:
            productos_no_disponibles = []
            no_disponible = f"No hay productos sin precios el dia {dt.datetime.now().day} de {nombre_mes}."
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
    if precios(lista_larga, dia1, dia2)[0] is False:
        mensaje = f"No hay precios del dia {dt.datetime.now().day} de {nombre_mes}."
        mensaje_max = None
        mensaje_min = None

        return mensaje, mensaje_max, mensaje_min, no_disponible

    # si es fin de mes y hay precios del dia actual
    elif es_fin_de_mes:
        mensaje = f"La variación de precios de la canasta básica en el mes de {nombre_mes} es del {variacion[0]}%."
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
        mensaje = f"La variación de precios de la canasta básica en el mes de {nombre_mes} al día {dt.datetime.now().day} es del {variacion[0]}%."
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
def twitear(lista_cantidad, dia1, dia2):
    """
    La función `twitear` envía tweets según los parámetros y condiciones dados.

    :param lista_cantidad: Parametro que pasa a la funcion `mensaje_twitter` para definir cuantos productos se van a imprimir en el mensaje de Twitter.
    :param dia1: El parámetro "dia1" representa el primer día para el cual se desea calcular la variación de precios. Debe ser una fecha específica en el formato "AAAA-MM-DD"
    :param dia2: El parámetro "dia2" representa el segundo día para el cual se desea calcular la variación de precios. Se utiliza en la función "mensaje_twitter" para calcular la variación entre "dia1" y "dia2" y generar los mensajes correspondientes

    """
    mensajes = mensaje_twitter(lista_cantidad, dia1, dia2)

    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret_key,
        access_token=access_token,
        access_token_secret=secret_token,
    )
    if check_execution_status(fecha):
        try:
            if precios(lista_larga, dia1, dia2)[0] is False:
                client.create_tweet(text=mensajes[0])
                print(mensajes[0])
            elif primer_dia:
                client.create_tweet(
                    text="Mes nuevo, precios nuevos!, a cruzar los dedos 🤞."
                )
                print("Mes nuevo, precios nuevos!, a cruzar los dedos 🤞.")
                if primer_dia_semana:
                    # variacion semana pasada
                    variacion_semana = variacion_personalizada(
                        primer_dia_semana_pasada.strftime("%Y-%m-%d"),
                        ultimo_dia_semana_pasada.strftime("%Y-%m-%d"),
                    )
                    # variacion semana pasada con respecto a la anterior
                    variacion_semana_anterior = variacion_personalizada(
                        (primer_dia_semana_pasada - timedelta(weeks=1)).strftime(
                            "%Y-%m-%d"
                        ),
                        (ultimo_dia_semana_pasada - timedelta(weeks=1)).strftime(
                            "%Y-%m-%d"
                        ),
                    )
                    print(variacion_semana)
                    print(variacion_semana_anterior)
                    # variacion con respecto a la semana anterior a la anterior
                    diferencia_entre_semanas = round(
                        (variacion_semana[1] / variacion_semana_anterior[1] - 1) * 100,
                        2,
                    )
                    if diferencia_entre_semanas < 0:
                        bajo_subio = "bajó"
                    else:
                        bajo_subio = "subió"

                    # La variación de precios de la canasta básica la semana 39 bajó/subió 1.49% respecto a la semana 38.
                    mensaje_var_semanal = f"La variación de precios de la canasta básica la semana numero {semana_pasada} {bajo_subio} {diferencia_entre_semanas}% con respecto a la semana {semana_pasada-1}."
                    mensaje_var_intersemanal = f"La variación de precios de la canasta básica la semana numero {semana_pasada} es del {variacion_semana[0]}%."
                    print(mensaje_var_semanal)
                    print(mensaje_var_semanal)
                    client.create_tweet(text=mensaje_var_semanal)
                    client.create_tweet(text=mensaje_var_intersemanal)

            else:
                if primer_dia_semana:
                    # variacion semana pasada
                    variacion_semana = variacion_personalizada(
                        primer_dia_semana_pasada.strftime("%Y-%m-%d"),
                        ultimo_dia_semana_pasada.strftime("%Y-%m-%d"),
                    )
                    # variacion semana pasada con respecto a la anterior
                    variacion_semana_anterior = variacion_personalizada(
                        (primer_dia_semana_pasada - timedelta(weeks=1)).strftime(
                            "%Y-%m-%d"
                        ),
                        (ultimo_dia_semana_pasada - timedelta(weeks=1)).strftime(
                            "%Y-%m-%d"
                        ),
                    )
                    print(variacion_semana)
                    print(variacion_semana_anterior)
                    # variacion con respecto a la semana anterior a la anterior
                    diferencia_entre_semanas = round(
                        (variacion_semana[1] / variacion_semana_anterior[1] - 1) * 100,
                        2,
                    )
                    if diferencia_entre_semanas < 0:
                        bajo_subio = "bajó"
                    else:
                        bajo_subio = "subió"

                    # La variación de precios de la canasta básica la semana 39 bajó/subió 1.49% respecto a la semana 38.
                    mensaje_var_semanal = f"La variación de precios de la canasta básica la semana numero {semana_del_año-1} {bajo_subio} {diferencia_entre_semanas}% con respecto a la semana {semana_del_año-2}."
                    mensaje_var_intersemanal = f"La variación de precios de la canasta básica la semana numero {semana_del_año-1} es del {variacion_semana[0]}%."
                    print(mensaje_var_semanal)
                    print(mensaje_var_semanal)
                    client.create_tweet(text=mensaje_var_semanal)
                    client.create_tweet(text=mensaje_var_intersemanal)

                client.create_tweet(text=mensajes[0])
                client.create_tweet(text=mensajes[1])
                client.create_tweet(text=mensajes[2])

                print(mensajes[0])
                print(mensajes[1])
                print(mensajes[2])

        except tweepy.TweepyException as e:
            print(e)
            print("Ya twitee hoy")
        Variable.set("twit_executed_today", fecha)
    else:
        print(f"Ya twitee hoy, {check_execution_status(fecha)} de chequeo")


# twiteo con tweepy

# twitear(lista_cantidad=4, dia1=primer_dia_mes_actual, dia2=fecha)
