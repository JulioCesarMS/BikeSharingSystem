import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import matplotlib.pyplot as plt
import mysql.connector
import seaborn as sns
from dbconnection import MySQLDatabase
from functions import guardar_filenames_txt, leer_filenames_txt, clean_data
import warnings
warnings.filterwarnings("ignore")



# conexión a base de datos
db = MySQLDatabase("ecobicis")


# funciones
def get_filnames_from_db(tabla):
    """
    Obtiene valores únicos del campo "Nombre_Archivo"
    """
    query = f"""SELECT DISTINCT Nombre_Archivo FROM {tabla};"""
    df = db.execute_query(query)
    # extraemos lista
    filenames_db = df['Nombre_Archivo'].to_list()
    return filenames_db


##########################################################################################################
#                                         función principal
##########################################################################################################
# tabal a actualizar
tabla = "viajes"
# verificamos cuantos archivos se cargaron dn base de datos
dbfilenames = get_filnames_from_db(tabla)
print(f'✅Total archivos en bd: {len(dbfilenames)}\n')

# inicia el web scrapping
web = "https://ecobici.cdmx.gob.mx/en/open-data/"
result = requests.get(web)
content = result.text

soup = BeautifulSoup(content, 'lxml')
url = 'https://ecobici.cdmx.gob.mx/'
datasets = []
filenames = []
box = soup.find('div', class_='elementor-container')
i = 0
for anio in  box.find_all('div', class_='elementor-toggle-item'):
  lista = anio.find('ul')
  # iteraos cada mes
  for mes in lista.find_all('a', href=True):
    #print(mes['href'])
    ruta = mes['href'].split('/')
    filename = ruta[-1]
    print(f"📂 Procesando: {filename}")
    # verificamos si el archivo existe en base de datos
    if (filename in dbfilenames) or (filename in filenames):
        print(f"❌ Archivo {filename} NO cargado a bd, ya existe \n")
        continue
    else:
        # si no existe lo agregamos
        filenames.append(filename)
        # lectura de archivo
        base = pd.read_csv(url + mes['href'], low_memory=False)
        # limpieza de archivo
        df = clean_data(base, nombre_archivo=filename)
        # carga a base de datos
        db.insertar_to_db(df, tabla, batch_size=5000)
        # imprime archivo insertados
        print(f"✅ Insertados {len(df)} registros de {filename}\n")
    i += 1

# guarda lista de archivo descargados
guardar_filenames_txt(filenames)
# verificamos cuantos archivos se cargaron dn base de datos
dbfilenames = get_filnames_from_db(tabla)
print(f'✅Total archivos cargados: {len(filenames)}')
# cerramos conexión
db.close()

