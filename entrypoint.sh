#!/bin/bash

# Inicializar la base de datos de Airflow
airflow db init

# Crear un usuario administrador si no existe
airflow users create \
    --username admin \
    --password admin \
    --firstname Nombre \
    --lastname Apellido \
    --role Admin \
    --email admin@example.com

# Iniciar el scheduler de Airflow en segundo plano
airflow scheduler &

# Iniciar el webserver de Airflow
exec airflow webserver
