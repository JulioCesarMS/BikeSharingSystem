
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import matplotlib.pyplot as plt
import seaborn as sns





def guardar_filenames_txt(filenames, archivo_salida="filenames_descargados.txt"):
    """
    Guarda una lista de filenames en un archivo .txt, un filename por línea.

    Parameters:
    filenames (list): Lista de nombres de archivos.
    archivo_salida (str): Nombre del archivo de salida .txt.
    """
    with open(archivo_salida, "w") as f:
        for fname in filenames:
            f.write(fname + "\n")
            
            
def leer_filenames_txt(archivo_entrada="filenames_descargados.txt"):
    """
    Lee un archivo .txt donde cada línea es un filename
    y devuelve una lista de filenames.

    Parameters:
    archivo_entrada (str): Nombre del archivo .txt a leer.

    Returns:
    list: Lista de filenames leídos del archivo.
    """
    with open(archivo_entrada, "r") as f:
        # Leemos cada línea, eliminando saltos de línea
        filenames = [line.strip() for line in f]
    return filenames



def clean_data(base, nombre_archivo):
    
    
    df = base.copy()
    df = df.iloc[:,:9]
    # renombramos
    df.columns = [
        "Genero_Usuario", "Edad_Usuario", "Bici",
        "Ciclo_Estacion_Retiro", "Fecha_Retiro", "Hora_Retiro",
        "Ciclo_Estacion_Arribo", "Fecha_Arribo", "Hora_Arribo"
    ]
    
    # Agregar columna del nombre del archivo
    df["Nombre_Archivo"] = nombre_archivo  

        # Genero: dejar solo M/F, lo demás = NaN
    df["Genero_Usuario"] = df["Genero_Usuario"].where(df["Genero_Usuario"].isin(["M", "F"]), np.nan)
    # Reemplazar strings vacíos o espacios en blanco con NaN en todas las columnas
    df = df.replace(r"^\s*$", np.nan, regex=True)
    # Convertir tipos de columnas (manejar errores → NaN)
    df["Edad_Usuario"] = pd.to_numeric(df["Edad_Usuario"], errors="coerce")
    df["Bici"] = pd.to_numeric(df["Bici"], errors="coerce")
    
    df["Ciclo_Estacion_Retiro"] = df["Ciclo_Estacion_Retiro"].astype(str).str.split("-").str[0]
    df["Ciclo_Estacion_Retiro"] = pd.to_numeric(df["Ciclo_Estacion_Retiro"], errors="coerce")
    df["Ciclo_Estacion_Arribo"] = df["Ciclo_Estacion_Arribo"].astype(str).str.split("-").str[0]
    df["Ciclo_Estacion_Arribo"] = pd.to_numeric(df["Ciclo_Estacion_Arribo"], errors="coerce")

    # df["Fecha_Retiro"] = pd.to_datetime(df["Fecha_Retiro"], errors="coerce", dayfirst=True).dt.date
    # df["Fecha_Arribo"] = pd.to_datetime(df["Fecha_Arribo"], errors="coerce", dayfirst=True).dt.date
    # df["Hora_Retiro"] = pd.to_datetime(df["Hora_Retiro"], errors="coerce", format="%H:%M:%S").dt.time
    # df["Hora_Arribo"] = pd.to_datetime(df["Hora_Arribo"], errors="coerce", format="%H:%M:%S").dt.time
    # Tomar primeros 10 caracteres de las fechas
    try:   
        df["Fecha_Retiro"] = pd.to_datetime(df["Fecha_Retiro"].astype(str), dayfirst=True, errors="coerce").dt.date
    except:
        df["Fecha_Retiro"] = pd.to_datetime(df["Fecha_Retiro"].astype(str), dayfirst=False, errors="coerce").dt.date
    
    try:
        df["Fecha_Arribo"] = pd.to_datetime(df["Fecha_Arribo"].astype(str), dayfirst=True, errors="coerce").dt.date
    except:
        df["Fecha_Arribo"] = pd.to_datetime(df["Fecha_Arribo"].astype(str), dayfirst=False, errors="coerce").dt.date

    # Limpiar horas 12h AM/PM, cambiar . por : si viene 12:01.59 a.m.
    df["Hora_Retiro"] = df["Hora_Retiro"].astype(str).str[:8]
    df["Hora_Retiro"] = pd.to_datetime(
        df["Hora_Retiro"].str.replace(r'\.', ':', regex=True),
        format="%H:%M:%S",
        errors="coerce"
    ).dt.time
    df["Hora_Arribo"] = df["Hora_Arribo"].astype(str).str[:8]
    df["Hora_Arribo"] = pd.to_datetime(
        df["Hora_Arribo"].str.replace(r'\.', ':', regex=True),
        format="%H:%M:%S",
        errors="coerce"
    ).dt.time
    
    return df

