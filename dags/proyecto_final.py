import subprocess

# Instalar python-dotenv usando pip desde el código
subprocess.run(["pip", "install", "python-dotenv"])
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import requests
import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from entrega.script import process_data, enviar_alerta_temperatura, enviar_alerta_volumen, etl_extraccion, etl_transformacion, etl_carga



#Cargar variables de entorno
load_dotenv()

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'juancruzgomez2000_coderhouse',
    'depends_on_past': False,
    'email': ['juancruzgomez2000@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2024, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creación del DAG
dag = DAG(
    dag_id='dag_clima_ciudades',
    default_args=default_args,
    description='DAG para extracción y transformación de datos climáticos de ciudades y carga en Redshift',
    schedule_interval='0 8/12 * * *',  # Corre dos veces al día: a las 8 AM y 8 PM
    catchup=False,
    concurrency=1,
    tags=['juan_cruz_gomez', 'data engineering', 'proyecto final', 'clima', 'ciudades']
)

# Tareas de ETL existentes
#1) EXTRACCION
def tarea_extraccion():
    lista_datos = etl_extraccion()
    return lista_datos

extraccion_tarea = PythonOperator(
    task_id='extraccion_datos',
    python_callable=tarea_extraccion,
    dag=dag
)

#2) TRANSFORMACION
def tarea_transformacion(ti):
    lista_datos = ti.xcom_pull(task_ids='extraccion_datos')
    datos_transformados = etl_transformacion(lista_datos)
    return datos_transformados

transformacion_tarea = PythonOperator(
    task_id='transformacion_datos',
    python_callable=tarea_transformacion,
    provide_context=True,
    dag=dag
)

#3) CARGA
def tarea_carga(ti):
    datos_transformados = ti.xcom_pull(task_ids='transformacion_datos')
    etl_carga(datos_transformados)

carga_tarea = PythonOperator(
    task_id='carga_redshift',
    python_callable=tarea_carga,
    provide_context=True,
    dag=dag
)

# FLUJO ETL
extraccion_tarea >> transformacion_tarea >> carga_tarea

# ALERTAS
# ALERTA LIMITE TEMPERATURA
alerta_temperatura_tarea = PythonOperator(
    task_id='alerta_temperatura',
    python_callable=enviar_alerta_temperatura,
    provide_context=True,
    dag=dag
)

# ALERTA LIMITE VOLUMEN DE DATOS
alerta_volumen_tarea = PythonOperator(
    task_id='alerta_volumen_datos',
    python_callable=enviar_alerta_volumen,
    provide_context=True,
    dag=dag
)

# Dependencias de alertas
transformacion_tarea >> alerta_temperatura_tarea
carga_tarea >> alerta_volumen_tarea

# Tarea adicional para disminuir el tiempo de espera e intentar solucionar el error
process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag
)

# Dependencia de la tarea adicional
transformacion_tarea >> process_task
