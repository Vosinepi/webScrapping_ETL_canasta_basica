import requests as req
import pandas as pd

from bs4 import BeautifulSoup
import re
import psycopg2
import datetime as dt
from datetime import datetime, timedelta


from certificados_ddbb import ddbb_pass


listado = {"fecha": dt.datetime.now().strftime("%Y-%m-%d")}

tabla = {"id": "SERIAL PRIMARY KEY", "fecha": "DATE"}


def kilo(nombre_producto, producto_url, porcion=1):
    valor = BeautifulSoup(producto_url.text, "html.parser")
    valor = valor.find_all("span", class_="unit")

    nombre = nombre_producto.replace(" ", "_")

    try:
        valor = valor[0].get_text()
    except IndexError:
        listado[nombre] = 0
        print(f"{nombre} IndexError, {listado[nombre]}")
        return None

    match = re.search(r"\$([\d,.]+)", valor)

    if match:
        number = float(match.group(1).replace(".", "").replace(",", "."))
        print(nombre, (number * porcion))
        listado[nombre] = number * porcion

    else:
        listado[nombre] = 0
        print("No se encontró un número en el string")


def unidad(nombre_producto, producto_url):
    valor = BeautifulSoup(producto_url.text, "html.parser")
    valor = valor.find_all("span", class_="atg_store_newPrice")

    nombre = nombre_producto.replace(" ", "_")

    try:
        valor = valor[0].get_text()
    except IndexError:
        listado[nombre] = 0
        print(f"{nombre} IndexError, {listado[nombre]}")
        return None

    match = re.search(r"\$([\d,.]+)", valor)

    if match:
        number = float(match.group(1).replace(".", "").replace(",", "."))
        print(nombre, number)
        listado[nombre] = number

    else:
        listado[nombre] = 0
        print("No se encontró un número en el string")


canasta = pd.read_csv(
    "listado canasta basica.csv",
    sep=";",
    encoding="latin-1",
    usecols=["producto", "cantidad_g_ml", "url_coto", "tipo_producto", "porcion"],
)


def scrapping(canasta):
    for producto in canasta.index:
        if canasta.loc[producto, "tipo_producto"] == "kilo":
            url = req.get(canasta.loc[producto, "url_coto"])

            if url.status_code == 200:
                kilo(canasta.loc[producto, "producto"], url)
            else:
                print("no hay url")

        elif canasta.loc[producto, "tipo_producto"] == "unidad":
            url = req.get(canasta.loc[producto, "url_coto"])

            if url.status_code == 200:
                unidad(canasta.loc[producto, "producto"], url)
            else:
                print("no hay url")

        else:
            canasta.loc[producto, "url_coto"] == "nan"
            print(producto, "no hay url")

    for producto in listado:
        if producto == "fecha":
            continue
        tabla[producto] = "FLOAT"


def cargar_ddbb_local(listado_productos):
    conn = psycopg2.connect(
        host="localhost",
        database="variacion",
        user="postgres",
        password="postgres",  # Cambiar por la contraseña de la base de datos creada
    )

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


def cargar_dddb_cloud(datos):
    # Conexión a la base de datos

    conn = psycopg2.connect(
        host="database-2.crdqtsbdpist.us-east-2.rds.amazonaws.com",
        database="postgres",
        user="postgres",
        password=ddbb_pass,
    )

    # Crear la tabla si no existe
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS precios_lista_larga (fecha DATE, producto VARCHAR(255), precio FLOAT)"
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

    print("Datos cargados")


def guardar_csv_excel():
    conn = psycopg2.connect(
        host="localhost", database="variacion", user="postgres", password="postgres"
    )
    global lista_larga
    # Obtener los datos de la tabla
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM precios")
        rows = cur.fetchall()
        conn.commit()

    # crear un dataframe con los datos
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    # Cerrar la conexión a la base de datos
    cur.close()
    conn.close()

    # Guardar el dataframe en un archivo csv
    df.to_csv("./datos/precios.csv", index=False)

    # guardar el dataframe en un archivo excel
    df.to_excel("./datos/precios.xlsx", index=False)

    lista_tabla = df.columns.to_list()

    lista_tabla.remove("id")
    lista_tabla.remove("fecha")

    lista_larga = df.melt(
        id_vars=["fecha"],
        value_vars=lista_tabla,
        var_name="producto",
        value_name="precio",
    )
    lista_larga.to_csv("./datos/precios_lista_larga.csv", index=False)


# if __name__ == "__main__":
#     scrapping(canasta)
#     cargar_ddbb_local(listado)
#     guardar_csv_excel()
#     cargar_dddb_cloud(lista_larga)


def etl_canasta():
    # Scrapping
    scrapping(canasta)

    # Cargar datos en la base de datos local
    cargar_ddbb_local(listado)

    # Guardar datos en csv y excel
    guardar_csv_excel()

    # Cargar datos en la base de datos cloud
    cargar_dddb_cloud(lista_larga)


etl_canasta()
