import datetime as dt


from variacion_perso import variacion_personalizada, fecha_actual
from fechas import (
    semana_del_año,
    primer_dia_semana_actual,
    primer_dia_semana_pasada,
    ultimo_dia_semana_pasada,
)


if primer_dia_semana_actual == dt.datetime.now().strftime("%Y-%m-%d"):
    variacion_semanal = variacion_personalizada(
        primer_dia_semana_pasada, ultimo_dia_semana_pasada
    )
    print(f"Primer dia de la semana {semana_del_año}")
else:
    print(f"\nNo es el primer dia de la semana actual {semana_del_año}\n")
    variacion = variacion_personalizada(
        primer_dia_semana_pasada, ultimo_dia_semana_pasada
    )
    print(f"Pero la variacion de la semana pasada es: {variacion} \n")
    print(f"pasaron {primer_dia_semana_actual - dt.datetime.now()} días")
