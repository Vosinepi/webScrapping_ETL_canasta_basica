import pandas as pd
from bs4 import BeautifulSoup
import re
import psycopg2
import datetime as dt
from datetime import datetime, timedelta
import shutil
import sys
import os
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from airflow import DAG  # type: ignore
from airflow.decorators import task, dag  # type: ignore

from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.email import EmailOperator  # type: ignore

# Agregar la carpeta 'plugins' al PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "plugins")))

# from airflow.dags.certificados_ddbb import ddbb_pass, host, user, database}
from certificados_ddbb import ddbb_pass, host, user, database  # type: ignore
from variacion_perso import variacion_personalizada, lista_variacion, lista_larga  # type: ignore

from telegfram_bot import telegram_bot_sendtext  # type: ignore

from fechas import (
    fecha,
    primer_dia_mes_actual,
    nombre_mes,
)  # type: ignore

from bot_twit import (
    twitear,
    mensaje_twitter,
    check_execution_status,
)

listado = {
    "fecha": dt.datetime.now().strftime("%Y-%m-%d")
}  # diccionario con los precios de los productos

tabla = {
    "id": "SERIAL PRIMARY KEY",
    "fecha": "DATE",
}  # diccionario con el nombre de las columnas de la tabla

# lista de productos Canasta basica
canasta = pd.read_csv(
    "/opt/airflow/data/listado canasta basica.csv",
    sep=";",
    encoding="utf-8",
    usecols=["producto", "cantidad_g_ml", "url_coto", "tipo_producto", "porcion"],
)

variacion = variacion_personalizada(primer_dia_mes_actual, fecha)

index_error = []  # lista de productos que no se encuentran en la pagina web


def kilo(nombre_producto, pagina, porcion=1):
    """
    Toma un nombre de producto y una URL, y devuelve el precio del producto.

    :param nombre_producto: El nombre del producto
    :param producto_url: La URL del producto
    :param porcion: la cantidad del producto que desea comprar, defaults to 1 (optional)
    :return: el valor de la variable "listado"
    """
    # pagina = BeautifulSoup(producto_url.page_source, "html.parser")
    # pagina = pagina.get_text
    product_info_container = pagina.find_all("div", id="productInfoContainer")
    disponibilidad = product_info_container[0].find_all(
        "div", class_="product_not_available"
    )

    disponibilidad = product_info_container[0].find_all(
        "div", class_="product_not_available"
    )

    nombre = nombre_producto.replace(" ", "_")
    if nombre == "SalchichÂ¢n":
        nombre = "salchich¢n"

    if disponibilidad:
        listado.update({nombre: 0})
        print(f"{nombre} no disponible, {listado[nombre]}")
        return None

    try:
        # busco dentro de product_info_container el span con la clase unit
        valor = product_info_container[0].find_all("span", class_="unit")[0].get_text()
    except IndexError:
        listado.update({nombre: 0})
        index_error.append(nombre)
        print(f"{nombre} IndexError, {listado[nombre]}")
        return None

    match = re.search(r"\$([\d,.]+)", valor)

    if match:
        number = float(match.group(1).replace(".", "").replace(",", "."))
        print(nombre, (number * porcion))

        listado.update({nombre: number * porcion})
    else:
        listado.update({nombre: 0})
        print("No se encontró un número en el string")


def unidad(nombre_producto, pagina, porcion=1):
    """
    Toma un nombre de producto y una URL, y devuelve el precio del producto.

    :param nombre_producto: nombre del producto
    :param producto_url: La URL del producto
    :return: el valor de la variable "número"
    """
    # pagina = BeautifulSoup(producto_url.page_source, "html.parser")
    # pagina = pagina.get_text
    product_info_container = pagina.find_all("div", id="productInfoContainer")
    # dentro del product_info_container busco el div con la clase product_not_available
    disponibilidad = product_info_container[0].find_all(
        "div", class_="product_not_available"
    )

    nombre = nombre_producto.replace(" ", "_")
    if nombre == "SalchichÂ¢n":
        nombre = "salchich¢n"
    print(f"El nombres es {nombre}")

    if disponibilidad:
        listado.update({nombre: 0})
        print(f"{nombre} no disponible, {listado[nombre]}")
        return None

    try:
        valor = (
            product_info_container[0]
            .find_all("span", class_="atg_store_newPrice")[0]
            .get_text()
        )
    except IndexError:
        listado.update({nombre: 0})
        index_error.append(nombre)
        print(f"{nombre} IndexError, {listado[nombre]}")
        return None

    match = re.search(r"\$([\d,.]+)", valor)

    if match:
        number = float(match.group(1).replace(".", "").replace(",", "."))
        print(nombre, number * porcion)
        listado.update({nombre: number * porcion})

    else:
        listado.update({nombre: 0})
        print("No se encontró un número en el string")


def scrapping(canasta):
    """
    Toma un marco de datos como entrada, y para cada fila en el marco de datos, llama a una función que
    extrae un sitio web y devuelve un valor.

    :param canasta: un marco de datos con los productos a raspar
    """
    # chromedriver path
    remote_webdriver = "remote_chromedriver"

    # Instantiate ChromeOptions
    chrome_options = webdriver.ChromeOptions()

    # Activate headless mode
    chrome_options.add_argument("--headless=new")

    # Instantiate a webdriver instance
    # driver = webdriver.Chrome(options=chrome_options, executable_path=chromedriver)
    driver = webdriver.Remote(f"{remote_webdriver}:4444/wd/hub", options=chrome_options)

    def status():
        try:
            status = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            return True
        except TimeoutException:
            print("TimeoutException")
            driver.quit()
            return None

    for producto in canasta.index:
        if canasta.loc[producto, "tipo_producto"] == "kilo":

            if status() == True:
                print(canasta.loc[producto, "url_coto"])
                driver.get(canasta.loc[producto, "url_coto"])

                driver.implicitly_wait(10)
                pagina = BeautifulSoup(driver.page_source, "html.parser")

                kilo(canasta.loc[producto, "producto"], pagina)
            else:
                print("no hay url")

        elif canasta.loc[producto, "tipo_producto"] == "unidad":
            # dejo la verificacion en false para que no me de error de certificado pero no es la mejor practica

            if status() == True:
                print(canasta.loc[producto, "url_coto"])
                driver.get(canasta.loc[producto, "url_coto"])

                driver.implicitly_wait(10)
                pagina = BeautifulSoup(driver.page_source, "html.parser")

                unidad(canasta.loc[producto, "producto"], pagina)
            else:
                print("no hay url")

        else:
            canasta.loc[producto, "url_coto"] = "nan"
            print(producto, "no hay url")
    if "nan" in listado.values():
        print("hay un Nan en el diccionario")
    print(f"Listado desde scrappin \n{listado}")

    for producto in listado:
        if producto == "fecha":
            continue
        tabla[producto] = "FLOAT"


def guardar_csv_excel():
    """
    Se conecta a una base de datos de Postgres, obtiene los datos de una tabla, crea un marco de datos,
    guarda el marco de datos como un archivo csv y excel, y luego crea un nuevo marco de datos con los
    datos en formato largo.
    """
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="variacion",
        user="postgres",
        password="postgres",
    )
    global lista_larga
    # Obtener los datos de la tabla
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM precios")
        rows = cur.fetchall()
        conn.commit()

    # si la tabla tiene nan salir
    print(f"Listado desde guardar_csv_excel \n {rows}")
    if "nan" in rows:
        print("hay un Nan en la tabla")
        return None

    # crear un dataframe si no existe o actualizarlo

    try:
        df = pd.read_csv("/opt/airflow/data/precios.csv")

        for row in rows:
            fecha = row[1].strftime("%Y-%m-%d")

            if fecha not in df["fecha"].values:
                df = df.append(
                    pd.DataFrame([row], columns=[desc[0] for desc in cur.description]),
                    ignore_index=True,
                )  # type: ignore
                df["fecha"] = pd.to_datetime(df["fecha"])
                df = df.sort_values(by="fecha")

                df = df.drop_duplicates()
                df = df.reset_index(drop=True)
                print("actualizando datos")
            else:
                print("no hay datos nuevos")
                continue

    except FileNotFoundError:
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
        print("creando dataframe")

    # Cerrar la conexión a la base de datos
    cur.close()
    conn.close()

    # Guardar el dataframe en un archivo csv
    df.to_csv("/opt/airflow/data/precios.csv", index=False)

    # guardar el dataframe en un archivo excel
    df.to_excel("/opt/airflow/data/precios.xlsx", index=False)

    lista_tabla = df.columns.to_list()

    lista_tabla.remove("id")
    lista_tabla.remove("fecha")

    # Crear un dataframe con los datos en formato largo
    lista_larga = df.melt(
        id_vars=["fecha"],
        value_vars=lista_tabla,
        var_name="producto",
        value_name="precio",
    )
    lista_larga.to_csv("/opt/airflow/data/precios_lista_larga.csv", index=False)
    lista_larga["fecha"] = pd.to_datetime(lista_larga["fecha"])


def cargar_dddb_cloud(datos):
    """
    Se conecta a una base de datos, crea una tabla si no existe y luego inserta datos en la tabla si aún
    no existe.

    :param datos: el dataframe que quiero subir a la base de datos
    """
    print(f"Listado desde cargar_dddb_cloud \n {datos}")
    if "nan" in datos["precio"]:
        print("hay un Nan en el diccionario")
        return
    # Conexión a la base de datos

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=ddbb_pass,
    )

    # Crear la tabla si no existe
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS precios_lista_larga (fecha DATE, producto VARCHAR(255), precio FLOAT, id SERIAL PRIMARY KEY)"
        )
        conn.commit()

        # Insertar los datos en la tabla

        for i in datos.index:
            if datos.loc[i, "fecha"].strftime("%Y-%m-%d") == dt.datetime.now().strftime(
                "%Y-%m-%d"
            ):
                producto_fecha = cur.execute(
                    "SELECT fecha, producto FROM precios_lista_larga WHERE fecha = %s AND producto = %s",
                    (datos.loc[i, "fecha"], datos.loc[i, "producto"]),
                )
                producto_fecha = cur.fetchone()
                print(producto_fecha)

                if producto_fecha == None:
                    print(f'no existe, cargando {datos.loc[i, "producto"]}')

                    cur.execute(
                        "INSERT INTO precios_lista_larga (fecha, producto, precio) VALUES (%s, %s, %s)",
                        (
                            datos.loc[i, "fecha"],
                            datos.loc[i, "producto"],
                            datos.loc[i, "precio"],
                        ),
                    )
                    conn.commit()
                else:
                    print(
                        f'ya existe {datos.loc[i, "producto"]}, {datos.loc[i, "fecha"]}'
                    )
                    continue
            else:
                continue

    # Cerrar la conexión a la base de datos
    cur.close()
    conn.close()

    print("DATOS")


def cargar_ddbb_local(listado_productos):
    """
    Si la tabla no existe, créela. Si la tabla existe, verifique si la fecha ya está en la tabla. Si no
    es así, inserte los datos. Si es así, no hagas nada.

    :param listado_productos: un diccionario los productos y el precio
    """
    print(f"Listado desde cargar_ddbb_local \n{listado_productos}")
    if "null" in listado_productos.values():
        print("hay un Nan en el diccionario")
        return
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="variacion",
        user="postgres",
        password="postgres",
    )
    # Crear la tabla si no existe
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS precios ({', '.join([f'{columna} {tipo}' for columna, tipo in tabla.items()])})"
        )
        conn.commit()

    # Insertar los datos en la tabla
    with conn.cursor() as cur:
        columnas = []
        valores = []
        for columna, valor in listado_productos.items():
            columnas.append(columna)
            valores.append(valor)

        data_fecha = cur.execute(
            "SELECT fecha FROM precios WHERE fecha = %s", (valores[0],)
        )
        data_fecha = cur.fetchone()

        if data_fecha == None:
            print("no hay fecha")
            print("Cargando datos")
            query = f"INSERT INTO precios ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(valores))})"
            cur.execute(query, valores)
            conn.commit()
        else:
            print("Ya se cargaron los datos de hoy")

    # Cerrar la conexión a la base de datos
    cur.close()
    conn.close()


def backup():
    """
    Hace un backup de los archivos csv y excel
    """
    # Hacer backup de los archivos csv y excel
    fecha = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    print("Haciendo backup de los archivos csv y excel")
    try:
        shutil.copy(
            "/opt/airflow/data/precios.csv",
            f"/opt/airflow/data/backup/precios{fecha}.csv",
        )
        shutil.copy(
            "/opt/airflow/data/precios.xlsx",
            f"/opt/airflow/data/backup/precios{fecha}.xlsx",
        )
        shutil.copy(
            "/opt/airflow/data/precios_lista_larga.csv",
            f"/opt/airflow/data/backup/precios_lista_larga{fecha}.csv",
        )
    except:
        print("Error al hacer el backup")

    print("Backup realizado")


def task_scrapping():
    scrapping(canasta)

    # Cargar datos y backup en la base de datos local

    # @task
    # def cargar_todo():
    cargar_ddbb_local(listado)

    # Guardar datos en csv y excel

    guardar_csv_excel()

    # Cargar datos en la base de datos cloud

    cargar_dddb_cloud(lista_larga)


# etl()

# Definir DAG

dag = DAG(
    dag_id="canasta_dag",
    description="DAG para scrapping de canasta familiar",
    schedule_interval="30 11 * * *",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=20),
        "start_date": datetime(2023, 3, 16),
        "email": ["ismaelpiovani@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": True,
    },
    catchup=False,
)


# Tarea para hacer backup de los archivos csv y excel
t0 = PythonOperator(
    task_id="backup",
    python_callable=backup,
    dag=dag,
)
# Tarea para scrapping

t1 = PythonOperator(
    task_id="tarea_scrapping",
    python_callable=task_scrapping,
    dag=dag,
)

t2 = EmailOperator(
    task_id="send_email",
    to="ismaelpiovani@gmail.com",
    subject="Airflow backup tarea canasta completada",
    html_content=""" <h3>Tareas completas</h3> """,
    dag=dag,
)

t3 = PythonOperator(
    task_id="productos_variacion",
    python_callable=lista_variacion,
    op_kwargs={"dia1": primer_dia_mes_actual, "dia2": fecha, "cantidad": 4},
    dag=dag,
)

# t4 = PythonOperator(
#     task_id="twitear",
#     python_callable=twitear,
#     op_kwargs={"lista_cantidad": 4, "dia1": primer_dia_mes_actual, "dia2": fecha},
#     dag=dag,
# )

# t5 = PythonOperator(
#     task_id="telegram_bot",
#     python_callable=telegram_bot_sendtext,
#     op_kwargs={"lista_cantidad": 6, "dia1": primer_dia_mes_actual, "dia2": fecha},
#     dag=dag,
# )

t6 = EmailOperator(
    task_id="email",
    to="ismaelpiovani@gmail.com",
    subject="Bot_twitter_canasta_basica",
    html_content=f"""
    <h3>La variación de precios de la canasta básica en el mes de {nombre_mes} al día {dt.datetime.now().day} es del {variacion[0]}%</h3>
    <h4>Los productos con mayor aumento al día de hoy son:</h4>
    <p>{mensaje_twitter(4, primer_dia_mes_actual, fecha)[1]}</p>
    <h4>Los productos con mayor reducción de precio al día de hoy son:</h4>
    <p>{mensaje_twitter(4, primer_dia_mes_actual, fecha)[2]}</p>
    <h4>Los productos que no se encuetran en la pagina hoy son:</h4>
    <p>{mensaje_twitter(4, primer_dia_mes_actual, fecha)[3]}</p>
    <h4>La variable de chequeo de twit es:{check_execution_status(fecha)}</h4>
    """,
    dag=dag,
)


# t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6  # type: ignore

t0 >> t1 >> t2 >> t3 >> t6  # type: ignore
