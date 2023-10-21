import pandas as pd
import datetime as dt


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
def precios(lista, dia1, dia2):
    """
    La función "precios" toma una lista de precios, una fecha de inicio y una fecha de finalización, y
    devuelve los precios para las fechas de inicio y finalización.

    :param lista: El parámetro "lista" es un marco de datos que contiene información sobre precios
    :param dia1: El parámetro "dia1" representa el primer día del mes del que desea recuperar los
    precios
    :param dia2: El parámetro "dia2" representa el segundo día del mes del que desea recuperar los
    precios
    :return: two variables: precios_dia1 and precios_dia2.
    """

    # Precios del primer día del mes actual

    precios_dia1 = lista[lista["fecha"] == dia1]

    # precios del dia actual
    precios_dia2 = lista[lista["fecha"] == dia2]

    if precios_dia1.empty == False and precios_dia2.empty == False:
        return precios_dia1, precios_dia2

    else:
        print("No hay precios del dia actual")
        return False, False


# limpio los precios
def limpio_precios(dia1, dia2):
    """
    La función `limpio_precios` toma dos marcos de datos que representan precios en dos días diferentes,
    elimina productos con precios cero y devuelve los marcos de datos limpios.

    :param dia1: dia1 es un DataFrame que contiene los precios de los productos el primer día
    :param dia2: dia2 es un DataFrame que contiene los precios de los productos en el día actual. Tiene
    las columnas "producto" y "precio" que representan el nombre del producto y su precio
    respectivamente
    :return: La función `limpio_precios` devuelve dos dataframes: `dia1` y `dia2`.
    """
    if dia1 is False or dia2 is False:
        print("No hay precios del dia actual en limpio precios")
        return False, False
    else:
        mask1 = dia2["precio"] == 0
        mask2 = dia2.groupby("producto")["precio"].transform("first") == 0
        mask3 = dia1["precio"] == 0
        mask4 = dia1.groupby("producto")["precio"].transform("first") == 0

        # Eliminar los productos sin precio
        dia1 = dia1[dia1["precio"] != 0]
        dia1 = dia1[dia1["producto"].isin(dia2.loc[~mask2, "producto"])]

        dia2 = dia2[dia2["precio"] != 0]
        dia2 = dia2[dia2["producto"].isin(dia1.loc[~mask4, "producto"])]

        # Restablecer los índices
        dia1.reset_index(drop=True, inplace=True)
        dia2.reset_index(drop=True, inplace=True)

        return dia1, dia2


# saco la variacion de precios mensual al dia de la fecha
def variacion_precios(precios_limpios):
    """
    La función `variacion_precios` calcula la variación porcentual entre dos conjuntos de precios.

    :param precios_limpios: Se espera que el parámetro "precios_limpios" sea una lista que contenga dos
    diccionarios. Cada diccionario representa los precios de un día específico. Los diccionarios deben
    tener la siguiente estructura:
    :return: la variación de los precios en porcentaje.
    """
    if precios_limpios[0] is False or precios_limpios[1] is False:
        print("No hay precios del dia actual en variacion precios")
        variacion_no_precios = "Non hay variacion por que no hay precios"
        return variacion_no_precios
    else:
        precio_anterior = sum(precios_limpios[0]["precio"])
        precio_actual = sum(precios_limpios[1]["precio"])

        # calculo la variación

        variacion = round((precio_actual / precio_anterior - 1) * 100, 2)
        promedio_precio = round((precio_actual + precio_anterior) / 2, 2)
        # print(f"La variacion es {variacion}\n")
        return variacion, promedio_precio


def productos_mas_variacion(limpio_precios, cantidad):
    """
    La función `productos_mas_variacion` calcula la variación de precios para un conjunto determinado de
    productos y devuelve los productos con mayores y menores variaciones.

    :param limpio_precios: Una lista de diccionarios que contienen los precios de los productos para dos
    días diferentes. Cada diccionario representa los precios de un día específico y tiene las siguientes
    claves: "producto" (nombre del producto) y "precio" (precio del producto)
    :param cantidad: El parámetro "cantidad" representa la cantidad de productos con las variaciones de
    precio más altas y más bajas que desea recuperar
    :return: La función `productos_mas_variacion` devuelve dos diccionarios: `mas_variacion` y
    `menor_variacion`. Estos diccionarios contienen los productos con mayor y menor variación de precio,
    respectivamente. Las claves de los diccionarios son los nombres de los productos, y los valores son
    las variaciones de precios correspondientes redondeadas a dos decimales.
    """
    print(cantidad)
    precios_primer_dia_mes_actual = limpio_precios[0]
    print(precios_primer_dia_mes_actual)
    precios_dia_actual = limpio_precios[1]
    print(precios_dia_actual)
    # calculo la variacion de precios
    if precios_primer_dia_mes_actual is False or precios_dia_actual is False:
        print("No hay precios del dia actual en productos mas variacion")
        mas_variacion = "No hay precios del dia actual en productos mas variacion"
        menor_variacion = "No hay precios del dia actual en productos mas variacion"
        return mas_variacion, menor_variacion

    else:
        precios_primer_dia_mes_actual["variacion"] = (
            precios_dia_actual["precio"] / precios_primer_dia_mes_actual["precio"] - 1
        ) * 100

        # ordeno los productos por variacion
        precios_primer_dia_mes_actual.sort_values(
            by=["variacion"], ascending=False, inplace=True
        )

        # me quedo con los 4 productos con mas variacion
        productos_mas_variacion = precios_primer_dia_mes_actual.head(cantidad)
        productos_menor_variacion = precios_primer_dia_mes_actual.tail(cantidad)

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


def variacion_personalizada(dia1, dia2):
    """
    La función `variacion_personalizada` calcula la variación personalizada entre dos días determinados
    utilizando una lista de precios.

    :param dia1: El parámetro "dia1" representa el día de inicio para el cual se desea calcular la
    variación
    :param dia2: El parámetro "dia2" representa la fecha de finalización del cálculo de variación de
    precio
    :return: la variable "variación", que representa la variación personalizada de precios.
    """
    variaciones = variacion_precios(
        limpio_precios(
            precios(lista_larga, dia1, dia2)[0], precios(lista_larga, dia1, dia2)[1]
        )
    )
    return variaciones


def lista_variacion(dia1, dia2, cantidad):
    """
    La función `lista_variacion` toma dos fechas y una cantidad como entrada, y devuelve una lista de
    productos con la mayor variación de precio dentro de ese período de tiempo.

    :param dia1: El parámetro "dia1" representa el día de inicio para el cual se desea calcular la
    variación de precios
    :param dia2: El parámetro "dia2" representa la fecha de finalización del período para el cual se
    desea calcular la variación
    :param cantidad: El parámetro "cantidad" representa la cantidad de productos con mayor variación que
    desea incluir en la lista resultante
    :return: the variable "lista_variacion".
    """
    lista_variacion = productos_mas_variacion(
        limpio_precios(
            precios(lista_larga, dia1, dia2)[0], precios(lista_larga, dia1, dia2)[1]
        ),
        cantidad,
    )
    return lista_variacion


def variaciones_semanales(semana1, semana2):
    # tomamos dos numeros de semana del año y calculamos la variacion de precios de cada una y la diferecia entre ellas.
    primer_dia_año = dt.date(fecha_actual.year, 1, 1)
    # fechas de la semana1
    dias_para_lunes = (7 - primer_dia_año.weekday()) % 7
    primer_dia_semana1 = primer_dia_año + dt.timedelta(
        days=dias_para_lunes + (semana1 - 1) * 7
    )
    ultimo_dia_semana1 = primer_dia_semana1 + dt.timedelta(days=6)

    # fechas de la semana2
    primer_dia_semana2 = primer_dia_año + dt.timedelta(
        days=dias_para_lunes + (semana2 - 1) * 7
    )
    ultimo_dia_semana2 = primer_dia_semana2 + dt.timedelta(days=6)

    # calculo la variacion de precios de cada semana
    variacion_semana1 = variacion_personalizada(
        primer_dia_semana1.strftime("%Y-%m-%d"), ultimo_dia_semana1.strftime("%Y-%m-%d")
    )
    variacion_semana2 = variacion_personalizada(
        primer_dia_semana2.strftime("%Y-%m-%d"), ultimo_dia_semana2.strftime("%Y-%m-%d")
    )

    # calculo la diferencia entre las variaciones
    diferencia = round(
        (variacion_semana1[1] / variacion_semana2[1] - 1) * 100,
        2,
    )

    return variacion_semana1, variacion_semana2, diferencia


print(variaciones_semanales(38, 37))
