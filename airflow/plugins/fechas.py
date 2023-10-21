import datetime as dt

# fechas variables
fecha_actual = dt.datetime.now()
fecha = dt.datetime.now().strftime("%Y-%m-%d")

# Primer dia del mes actual
primer_dia_mes_actual = dt.datetime.now().replace(day=1).strftime("%Y-%m-%d")

# semana del mes
semana = fecha_actual.isocalendar()[1]

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

# Obtener la semana del año
semana_del_año = fecha_actual.isocalendar()[1]
print(f"Semana del año: {semana_del_año}")

# Obtener la primer fecha de la semana actual
primer_dia_semana_actual = fecha_actual - dt.timedelta(days=fecha_actual.weekday())

print(f"Primer día de la semana actual: {primer_dia_semana_actual}")

# Obtener la primer fecha de la semana pasada
primer_dia_semana_pasada = primer_dia_semana_actual - dt.timedelta(weeks=1)
# primer_dia_semana_pasada = primer_dia_semana_pasada.strftime("%Y-%m-%d")
print(f"Primer día de la semana pasada: {primer_dia_semana_pasada}")

# Obtener la última fecha de la semana pasada (6 días antes del primer día de la semana actual)
ultimo_dia_semana_pasada = primer_dia_semana_actual - dt.timedelta(days=1)
# ultimo_dia_semana_pasada = ultimo_dia_semana_pasada.strftime("%Y-%m-%d")
print(f"Último día de la semana pasada: {ultimo_dia_semana_pasada}")

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
print(primer_dia_semana_actual)
primer_dia_semana = primer_dia_semana_actual == dt.datetime.now()
print(primer_dia_semana)
