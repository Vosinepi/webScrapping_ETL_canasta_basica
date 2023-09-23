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
    # Precios del primer día del mes actual
    if fecha == lista["fecha"].max():
        precios_dia1 = lista_larga[lista_larga["fecha"] == dia1]

        # precios del dia actual
        precios_dia2 = lista_larga[lista_larga["fecha"] == dia2]

        return precios_dia1, precios_dia2

    else:
        print("No hay precios del dia actual")
        return False, False


# limpio los precios
def limpio_precios(dia1, dia2):
    if dia1 is False or dia2 is False:
        print("No hay precios del dia actual")
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
    if precios_limpios[0] is False or precios_limpios[1] is False:
        print("No hay precios del dia actual")
        return False
    else:
        precio_anterior = sum(precios_limpios[0]["precio"])
        precio_actual = sum(precios_limpios[1]["precio"])

        # calculo la variación

        variacion = round((precio_actual / precio_anterior - 1) * 100, 2)
        # print(f"La variacion es {variacion}\n")
        return variacion


def productos_mas_variacion(limpio_precios, cantidad):
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
    variacion = variacion_precios(
        limpio_precios(
            precios(lista_larga, dia1, dia2)[0], precios(lista_larga, dia1, dia2)[1]
        )
    )
    return variacion


def lista_variacion(dia1, dia2, cantidad):
    lista_variacion = productos_mas_variacion(
        limpio_precios(
            precios(lista_larga, dia1, dia2)[0], precios(lista_larga, dia1, dia2)[1]
        ),
        cantidad,
    )
    return lista_variacion
