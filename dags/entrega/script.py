#####1) Librerías a considerar
import subprocess

# Instalar python-dotenv usando pip desde el código
subprocess.run(["pip", "install", "python-dotenv"])
import pandas as pd
import psycopg2
import requests
import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


#####2) Carga de variables de entorno desde .env
load_dotenv()

#####3) Información extraída de 2 fuentes de datos.
info_pais = pd.read_excel('/opt/airflow/dags/entrega/info_pais.xlsx')
info_ciudad = pd.read_excel('/opt/airflow/dags/entrega/info_ciudad.xlsx')
# Agrupo por país
ciudades_por_pais = info_ciudad.groupby('pais').apply(lambda group: group.to_dict(orient='records')).to_dict()

######4) VARIABLES SECRETS utilizando .env

#a) Acceso en Redshift
CREDENCIALES_REDSHIFT = {
    'user': os.getenv('REDSHIFT_USER'),
    'password': os.getenv('REDSHIFT_PASSWORD'),
    'host': os.getenv('REDSHIFT_HOST'),
    'port': os.getenv('REDSHIFT_PORT'),
    'database': os.getenv('REDSHIFT_DATABASE')
}

#b) API key de HG Brasil
HG_BRASIL_API_KEY = os.getenv('HG_BRASIL_API_KEY')

#c) API key de SendGrid y correos electrónicos
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
EMAIL_TO = os.getenv('EMAIL_TO')
EMAIL_FROM = os.getenv('EMAIL_FROM')

######5) Otras variables

#a)API de HG Brasil
woeids = [466862, 395269, 107681, 9807, 44418, 868274, 1940345, 2151849, 1582504, 1105779]

#b)Tratamiento de valores nulos
variables_eliminar = ['city', 'temp', 'date', 'time', 'humidity', 'cloudiness', 'rain', 'wind_speedy', 'wind_direction']
variables_predeterminado = {
    'condition_code': 'Dato no disponible',
    'description': 'Dato no disponible',
    'currently': 'Dato no disponible',
    'wind_cardinal': 'Dato no disponible',
    'sunrise': 'Dato no disponible',
    'sunset': 'Dato no disponible',
    'condition_slug': 'Dato no disponible',
    'moon_phase': 'Dato no disponible',
    'timezone': 'Dato no disponible'
}

#c)Límites para alertas
UMBRAL_TEMPERATURA = 40
UMBRAL_VOLUMEN = 8

#####6) Funciones
def process_data():
    """
    Función para disminuir el tiempo de espera e intentar solucionar el error
    """
    file_path = '/opt/airflow/dags/info_pais.xlsx'
    try:
        df = pd.read_excel(file_path)
        # Realiza el procesamiento necesario
        print(df.head())
    except FileNotFoundError as e:
        print(f"Error: {e}")

def conectar_redshift():
    """
    Conexión a Redshift utilizando las credenciales definidas.
    """
    return psycopg2.connect(**CREDENCIALES_REDSHIFT)

def crear_tabla_redshift(conn):
    """
    Verificación de la existencia de la tabla en Redshift.
    Si no existe --> Creación.
    """
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'datos_ciudades_proyecto_final'
    ''')
    existe_tabla = cursor.fetchone()[0]
    if not existe_tabla:
        cursor.execute('''
            CREATE TABLE datos_ciudades_proyecto_final (
                id_registro INTEGER IDENTITY(1,1) PRIMARY KEY,
                ciudad VARCHAR(255),
                continente VARCHAR(50),
                pais VARCHAR(50),
                capital_pais VARCHAR(2),
                sup_pais_km2 INTEGER,
                sup_ciudad_km2 INTEGER,
                habitantes INTEGER,
                clima VARCHAR(50),
                fecha_consulta DATE,
                horario TIME,
                momento VARCHAR(50),
                temperatura INTEGER,
                descripcion VARCHAR(255),
                condicion_codigo VARCHAR(10),
                humedad INTEGER,
                nubosidad DECIMAL(5, 2),
                lluvia_mm DECIMAL(5, 2),
                velocidad_viento VARCHAR(20),
                direccion_grados_viento INTEGER,
                direccion_cardinal_viento VARCHAR(10),
                amanecer TIME,
                puesta_sol TIME,
                meteorologica_actual VARCHAR(50),
                fase_lunar VARCHAR(50),
                zona_horaria VARCHAR(20),
                fecha_carga TIMESTAMP DEFAULT GETDATE()
            )
        ''')
        conn.commit()
        print("Tabla 'datos_ciudades_proyecto_final' creada exitosamente en Redshift.")
    cursor.close()

def extraccion_datos(url):
    """
    Solicitud GET a la URL proporcionada para extraer datos de la API pública.
    Si la solicitud es exitosa
    Retorno de los datos en formato JSON
    Si la solicitud no es exitosa
    Impresión de error
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al extraer datos: {e}")
        return None

def transformacion_datos(data):
    """
    Transformación de los datos extraídos de la API pública
        Eliminación de variables innecesarias.
        Datos transformados en formato específico.
    """
    datos_transformados = {}
    for key in ['forecast', 'cid', 'img_id', 'city_name', 'latitude', 'longitude', 'cref']:
        data.pop(key, None)
    return data.get('results', {})

def combinar_datos(ciudades_por_pais, datos_transformados):
    """
    Combinación de:
        Datos de la API
        Datos de las fuentes de datos
    Luego datos combinados para cada ciudad y país.
    """
    datos_combinados = []
    for pais, ciudades in ciudades_por_pais.items():
        for ciudad in ciudades:
            ciudad_nombre = ciudad['ciudad']
            if ciudad_nombre in datos_transformados:
                datos_ciudad = datos_transformados[ciudad_nombre]
                datos_combinados.append({
                    'ciudad': ciudad_nombre,
                    'continente': info_pais.loc[info_pais['pais'] == pais, 'continente'].iloc[0],
                    'pais': pais,
                    'capital_pais': ciudad['capital_pais'],
                    'sup_pais_km2': info_pais.loc[info_pais['pais'] == pais, 'sup_pais_km2'].iloc[0],
                    'sup_ciudad_km2': ciudad['sup_km2'],
                    'habitantes': ciudad['habitantes'],
                    'clima': ciudad['clima'],
                    'fecha_consulta': datos_ciudad['date'],
                    'horario': datos_ciudad['time'],
                    'momento': datos_ciudad['currently'],
                    'temperatura': datos_ciudad['temp'],
                    'descripcion': datos_ciudad['description'],
                    'condicion_codigo': datos_ciudad['condition_code'],
                    'humedad': datos_ciudad['humidity'],
                    'nubosidad': datos_ciudad['cloudiness'],
                    'lluvia_mm': datos_ciudad['rain'],
                    'velocidad_viento': datos_ciudad['wind_speedy'],
                    'direccion_grados_viento': datos_ciudad['wind_direction'],
                    'direccion_cardinal_viento': datos_ciudad['wind_cardinal'],
                    'amanecer': datos_ciudad['sunrise'],
                    'puesta_sol': datos_ciudad['sunset'],
                    'meteorologica_actual': datos_ciudad['condition_slug'],
                    'fase_lunar': datos_ciudad['moon_phase'],
                    'zona_horaria': datos_ciudad['timezone']
                })
    return datos_combinados

def carga_datos_redshift(conn, datos_carga):
    """
    Carga de datos combinados en la tabla en Redshift
    Verificación de la existencia de registros previos
    No inserción de duplicados 
    """
    cur = conn.cursor()
    duplicados = []

    for data in datos_carga:
        cur.execute(
            "SELECT COUNT(*) FROM datos_ciudades_proyecto_final WHERE fecha_consulta = %s AND horario = %s AND ciudad = %s",
            (data['fecha_consulta'], data['horario'], data['ciudad'])
        )
        count = cur.fetchone()[0]

        if count > 0:
            duplicados.append(data)
        else:
            cur.execute(
                "INSERT INTO datos_ciudades_proyecto_final (ciudad, continente, pais, capital_pais, sup_pais_km2, sup_ciudad_km2, habitantes, clima, fecha_consulta, horario, momento, temperatura, descripcion, condicion_codigo, humedad, nubosidad, lluvia_mm, velocidad_viento, direccion_grados_viento, direccion_cardinal_viento, amanecer, puesta_sol, meteorologica_actual, fase_lunar, zona_horaria, fecha_carga) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    data['ciudad'],
                    data['continente'],
                    data['pais'],
                    data['capital_pais'],
                    data['sup_pais_km2'],
                    data['sup_ciudad_km2'],
                    data['habitantes'],
                    data['clima'],
                    data['fecha_consulta'],
                    data['horario'],
                    data['momento'],
                    data['temperatura'],
                    data['descripcion'],
                    data['condicion_codigo'],
                    data['humedad'],
                    data['nubosidad'],
                    data['lluvia_mm'],
                    data['velocidad_viento'],
                    data['direccion_grados_viento'],
                    data['direccion_cardinal_viento'],
                    data['amanecer'],
                    data['puesta_sol'],
                    data['meteorologica_actual'],
                    data['fase_lunar'],
                    data['zona_horaria'],
                    datetime.datetime.now()
                )
            )
    conn.commit()
    cur.close()
    return duplicados

def etl_extraccion():
    """
    Extracción de datos meteorológicos de múltiples localizaciones utilizando la API de HG Brasil.
    Retorna una lista de datos extraídos para cada WOEID en la lista `woeids`.
    """
    lista_datos = []
    for woeid in woeids:
        data = extraccion_datos(f'https://api.hgbrasil.com/weather?locale=en&woeid={woeid}&key={HG_BRASIL_API_KEY}')
        if data:
            lista_datos.append(data)
    return lista_datos

def etl_transformacion(lista_datos):
    """
    Transformación de los datos extraídos de la API de HG Brasil.
    Retorna un diccionario de datos transformados, donde cada clave es el nombre de la ciudad.
    """
    datos_transformados = [transformacion_datos(data) for data in lista_datos]
    return {ciudad['city_name']: ciudad for datos in datos_transformados for ciudad in datos}

def etl_combinar(ciudades_por_pais, datos_transformados):
    """
    Combinación de datos meteorológicos transformados con información geográfica por ciudad y país.
    Retorna datos combinados estructurados para cada ciudad y país.
    """
    return combinar_datos(ciudades_por_pais, datos_transformados)

def etl_carga(datos_combinados):
    """
    Carga de datos combinados en la tabla en Redshift después de conectar y crear la tabla si no existe.
    Retorna una lista de datos duplicados que no fueron insertados debido a su existencia previa.
    """
    conn = conectar_redshift()
    crear_tabla_redshift(conn)
    duplicados = carga_datos_redshift(conn, datos_combinados)
    conn.close()
    return duplicados

#####7) Lanzar alertas por e-mail.
#ALERTA 1. TEMPERATURA
def enviar_alerta_temperatura(ti):
    datos_transformados = ti.xcom_pull(task_ids='transformacion_datos')
    for ciudad, datos in datos_transformados.items():
        temperatura = datos['temperatura']
        if temperatura > UMBRAL_TEMPERATURA:
            enviar_email_alerta(f"Alerta de Temperatura en {ciudad}", f"La temperatura actual es {temperatura}°C, ha superado el umbral permitido.")

#ALERTA 2. VOLUMEN DE DATOS
def enviar_alerta_volumen(ti):
    datos_transformados = ti.xcom_pull(task_ids='transformacion_datos')
    if len(datos_transformados) < UMBRAL_VOLUMEN:
        enviar_email_alerta("Alerta de Volumen de Datos", f"Se han procesado solo {len(datos_transformados)} registros, no alcanzando el umbral de {UMBRAL_VOLUMEN}.")

#LANZAR ALERTA POR MAIL
def enviar_email_alerta(asunto, cuerpo):
    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=EMAIL_TO,
        subject=asunto,
        html_content=cuerpo)
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(str(e))

