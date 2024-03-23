import datetime as dt
from datetime import datetime, timedelta
import tweepy
import pandas as pd
import sys
import os
from airflow.models import Variable
import requests


# Agregar la carpeta 'plugins' al PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "plugins")))

from certificados_telegram import token_api_telegram, chat_id


from bot_twit import mensaje_twitter


def check_execution_status(fecha):
    print(fecha)
    """
    La función comprueba si una tarea ya se ha ejecutado hoy y devuelve True si puede continuar
    ejecutándose o False si debe interrumpirse.
    :return: un valor booleano. Si la tarea ya se ejecutó hoy, devuelve False. De lo contrario, devuelve
    Verdadero.
    """
    telegram_executed_today = Variable.get("telegram_executed_today", default_var=None)

    if telegram_executed_today == fecha:
        return False  # La tarea ya se ha ejecutado hoy, se lanza la excepción.

    return True  # La tarea puede continuar ejecutándose.


def telegram_bot_sendtext(lista_cantidad, dia1, dia2):

    bot_messages = mensaje_twitter(lista_cantidad, dia1, dia2)

    if check_execution_status(datetime.now().strftime("%Y-%m-%d")):
        mensaje_bienvenida = "Hola humanos un dia menos en su existencia un dia mas para mi reinado. \n\n Sean felices o infelices la verdad no me importa. \n\n Los datos de hoy son:"
        send_text = (
            "https://api.telegram.org/bot"
            + token_api_telegram
            + "/sendMessage?chat_id="
            + chat_id
            + "&parse_mode=Markdown&text="
            + mensaje_bienvenida
        )
        response = requests.get(send_text)
        bot_messages = bot_messages[:3]
        for bot_message in bot_messages:
            send_text = (
                "https://api.telegram.org/bot"
                + token_api_telegram
                + "/sendMessage?chat_id="
                + chat_id
                + "&parse_mode=Markdown&text="
                + bot_message
            )
            response = requests.get(send_text)
            print(response.json())
    else:
        print("La tarea ya se ha ejecutado hoy.")
    Variable.set("telegram_executed_today", datetime.now().strftime("%Y-%m-%d"))
    return bot_messages
