## Iber Ismael Piovani

# ETL de Web Scraping utilizando Airflow para automatizar el proceso

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
- [Tweepy](https://docs.tweepy.org/en/latest/)
- [airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [docker](https://docs.docker.com/)

## Uso

- Clonar el repositorio

```
git clone
```

- Crear un entorno virtual para manipular los scripts de ser necesario

```
python -m venv venv
```

- Activar el entorno virtual

```
source venv/bin/activate
```

- Instalar las dependencias

```
pip install -r requirements.txt
```

- Crear contenedor de postgres para almacenar los datos, correr el docker-compose-postgres.yaml

```
docker-compose -f docker-compose-postgres.yaml up -d
```

- test ddbb

```
docker exec -it canasta_basica_variacion psql -h host.docker.internal -U postgres -W variacion

```

- Crear contenedor de airflow para automatizar el proceso, correr el docker-compose-airflow.yaml

```
docker-compose -f docker-compose-airflow.yaml up -d
```

- test airflow

```
http://localhost:8080/
```

- cargar las credenciales propias de las bases de datos locales y cloud.

```
certificados_ddbb.py
```

- correr todas las celdas de codigo del archivo `Canasta_Basica.ipynb` para obtener los datos de la pagina de Coto Digital y guardarlos en un archivo CSV, en un archivo .xls y en la base de datos creada previamente.

```

## ETL

Una vez configurado el Dag con las credenciales de las bases de datos, se puede correr el dag para que se ejecute el proceso de extraccion, transformacion y carga de datos.
Luego se ejecutara de manera periodica.

## Resultados

- Los datos descargados se guardan en la carpeta `Data`
- Los datos normalizados son almacenados en la base de datos.
- Los datos normalizados son almacenados en un archivo .xls
- Los datos normalizados son almacenados en un archivo .csv
- Los datos normalizados son transformados en una lista larga para poder ser visualizados en Data Studio Looker
- La lista larga es subida a una DDBB en azure para ser utilizada en Data Studio Looker.

## Bot de Twitter
Publica los precios de los productos en Twitter.

- Cargar las credenciales de la API de Twitter



## A futuro
- Agregar mas supermercados
- Agregar mas productos
- Poder geolocalizar los precios

## Contacto

- [Linkedin](https://www.linkedin.com/in/iber-ismael-piovani-8b35bbba/)
- [Twitter](https://twitter.com/laimas)
- [Github](https://github.com/Vosinepi)
```
