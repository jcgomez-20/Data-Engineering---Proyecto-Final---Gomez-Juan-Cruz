# ETL de Datos Climáticos para Redshift

El script correspondiente a la entrega final se encuentra dentro de la carpeta *dag* y se llama **proyecto_final.py**
El script  **proyecto_final.py** "llama" al codigo que esta dentro de la carpeta *dag/entrega* y se llama **script.py**
En el **script.py** se lleva a cabo todo el proceso correspondiente a las entregas 1,2 y 3

Este script realiza un proceso ETL para extraer datos climáticos de varias ciudades utilizando la API de HG Brasil, transformarlos y cargarlos en una tabla en Redshift.

## Configuración

Título y Descripción del Proyecto: Curso Data Engineering. Proyecto Final. Gomez Juan Cruz
PARTE 1: ETL de datos meteorológicos utilizando API pública. 
PARTE 2: Creación de un DAG
PARTE 3: Uso de terminal, Docker y Airflow 

PARTE 1.
Visitar la página "https://hgbrasil.com/", registrarse y solicitar un API gratuita.    

- El procedimiento completo de ETL se puede visualizar en el script.py
- En el archivo secrets.env se encuentras las credenciales secretas para:
    - Conexión a RedShift
    - API KEY
    - Lanzamiento de alertas vía mail

1) EXTRACCIÓN.
A la hora de realizar una consulta a la API devuelve 
    - aproximadamente unas 20 variables
    - pronósticos para días siguientes.
    - datos en formato JSON
Además se extrae información de 2 fuentes de datos:
    - información en formato xlsx de información de las 8 ciudades
    - información en formato xlsx de información de los 8 países a los que refiere cada ciudad

2) TRANSFORMACIÓN.
Se realizan las siguietes transformaciones para los datos recopilados
    - Se descartan las variables que no interesan
    - Se descartan los pronósticos
    - Tratamiento de valores nulos y/o datos faltantes
Se combina la información de los datos provenientes de la API con la de las fuentes de datos

- AUTOMATIZACIÓN DEL PROCESO. LANZAMIENTO DE ALERTAS
Se notificará via mail 2 alertas especificadas en el script.

3) CARGA.
Carga de datos en el DataWarehouse (Base de datos en RedShift)

PARTE 2.
- **El procedimiento completo de utilizacion de DAGs se puede visualizar en el proyecto_final.py**

1) "Llamar" al script.py y tomar las funciones necesarias para el empleo de DAGs.
2) Creación del DAG con sus argumentos
3) Especificación de tareas de ETL
4) Alertas
5) Dependencia de alertas

PARTE 3. 
Los archivos dentro de la carpeta "Proyecto Final" hacen posible conectarse a http://localhost:8080/home y utilizar Airflow

1) Agrego el Archivo de origen Yaml docker-compose a la carpeta

2) Pasos en la terminal:
    1- cd "C:\Users\Seidor Analytics.SEIDOR136\Desktop\Curso Data Engineering\Entregas\Proyecto Final" --> directorio de trabajo
    2- docker-compose up --> se crean las carpetas dags y logs
Ubicar dentro de la carpeta dags:
    - entrega
        - info_pais.xlsx
        - info_ciudad.xlsx
        - script.py
    - proyecto_final.py
    3- docker build -t proyecto_final_imagen . --> Reconstruyo la imagen
	4- docker run -p 8080:8080 -d proyecto_final_imagen --> Ejecutar el contenedor


3) localhost:8080
	a- usuario y contraseña: airflow
	b- activo el dag y lo ejecuto

4) En Airflow se ejecutan las tareas de la siguiente manera:
- Tarea 1. Extraccion: Se extraen los datos desde la API
- Tarea 2. Transformacion
	-  Archivo JSON --> DataFrame
  	-  Se une la informacion de la API con las de las fuentes de datos adicionales
	-  Se descartan las variables que no interesan
	-  Se descarta los duplicados y se reemplazan los valores nulos
 	-  Se agrega una columna temporal que indica el momento en que se carga el dato
- Tarea 3. Carga: Se cargan los datos en la base de datos de RedShift
- Tarea 4. Alertas: Se envían alertas via mail en caso de ser necesario
