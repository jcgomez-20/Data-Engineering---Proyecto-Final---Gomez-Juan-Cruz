FROM python:3.9.5-slim-buster

# Crear el usuario airflow con UID y GID
RUN groupadd -g 50000 airflow && \
    useradd -u 50000 -g airflow -m -s /bin/bash airflow

# Instalar Apache Airflow
RUN pip install apache-airflow

# Establecer el directorio de trabajo en el contenedor
WORKDIR /opt/airflow

# Copiar los archivos de requisitos
COPY requirements.txt .

# Instalar las dependencias necesarias incluyendo python-dotenv
RUN pip install --no-cache-dir -r requirements.txt

# Copiar los DAGs al directorio de Airflow
COPY dags /opt/airflow/dags

# Copiar los archivos necesarios al contenedor
COPY dags/proyecto_final.py /opt/airflow/dags/proyecto_final.py
COPY dags/entrega/script.py /opt/airflow/dags/entrega/script.py
COPY dags/entrega/info_pais.xlsx /opt/airflow/dags/entrega/info_pais.xlsx
COPY dags/entrega/info_ciudad.xlsx /opt/airflow/dags/entrega/info_ciudad.xlsx

# Copiar el archivo .env al directorio de trabajo
COPY secrets.env /opt/airflow/

# Copiar el archivo entrypoint.sh
COPY entrypoint.sh /entrypoint.sh

# Cambiar los permisos del script de entrada con usuario root
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Configurar el punto de entrada
ENTRYPOINT ["/entrypoint.sh"]

# Exponer los puertos necesarios
EXPOSE 8080

# Iniciar el scheduler y el webserver
CMD ["webserver"]