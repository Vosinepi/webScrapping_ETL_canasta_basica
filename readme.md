## Iber Ismael Piovani

# Pequeño ETL de Web Scraping

## Data Studio Looker

Pequeño dash para poder visualizar la data con la capacidad de filtrar por producto y periodo de tiempo.

[Data Studio Looker](https://lookerstudio.google.com/reporting/836bfaaa-82ac-467f-b5e2-886cca7c97f4)

## Objetivo

Mediante el listado de Canasta Basica del INDEC obtenemos los precios de la pagina de una cadena de supermercados para ir almacenando los precios en una base de datos y poder analizarlos.

## Enlaces

- [Coto Digital](https://www.cotodigital3.com.ar/)
- [Canasta Basica INDEC](https://www.indec.gob.ar/ftp/cuadros/sociedad/EPH_metodologia_22_pobreza.pdf)

## Requerimientos

- Python 3.8

- [Pandas](https://pandas.pydata.org/docs/)
- [psycopg2](https://pypi.org/project/psycopg2/)
- [tabulate](https://pypi.org/project/tabulate/)
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [requests](https://requests.readthedocs.io/en/master/)

## Uso

- Clonar el repositorio

```
git clone
```

- Crear un entorno virtual

```
python -m venv venv
```

- correr Docker de SQL Server

```
docker run -d --name canasta_basica_variacion -v my_db:/var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=variacion postgres
```

test ddbb

```
docker exec -it canasta_basica_variacion psql -h localhost -U postgres -W variacion

```

- Instalar las dependencias

```
pip install -r requirements.txt
```

- cargar las credenciales de la base de datos en el archivo `crear_tabla.ipnyb` y correrlo para crear la tabla en la base de datos

```
Canasta_Basica.ipynb
```

- correr todas las celdas de codigo del archivo `Canasta_Basica.ipynb` para obtener los datos de la pagina de Coto Digital y guardarlos en un archivo CSV, en un archivo .xls y en la base de datos creada previamente.

```

## Resultados

- Los datos descargados se guardan en la carpeta `Data`
- Los datos normalizados son almacenados en la base de datos.

## A futuro
- Agregar mas supermercados
- Agregar mas productos
- Poder geolocalizar los precios

## Contacto

- [Linkedin](https://www.linkedin.com/in/iber-ismael-piovani-8b35bbba/)
- [Twitter](https://twitter.com/laimas)
- [Github](https://github.com/Vosinepi)
```
