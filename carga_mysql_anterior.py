import mysql
import mysql.connector
from mysql.connector import Error
import os
import pandas as pd  
import numpy as np 
import glob
import os



# 📌 Conexión MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="astro123",
    database="ecobicis"
)
cursor = connection.cursor()

# 📌 Carpeta con los archivos
folder = "C:/Users/HP/Desktop/Análisis México/Ecobicis/datasets"

# 📌 Recorremos todos los archivos 
for file in os.listdir(folder):
    if file.endswith(".csv"):
        path = os.path.join(folder, file)
        print(f"📂 Procesando: {file}")

        # Leer Excel
        df = pd.read_csv(path, dtype=str)  # leer todo como str para evitar problemas
        # seleccionamos primeras 9 columnas de 0 a 8
        df = df.iloc[:,:9]
        # renombramos
        df.columns = [
            "Genero_Usuario", "Edad_Usuario", "Bici",
            "Ciclo_Estacion_Retiro", "Fecha_Retiro", "Hora_Retiro",
            "Ciclo_Estacion_Arribo", "Fecha_Arribo", "Hora_Arribo"
        ]
        
        # Agregar columna del nombre del archivo
        df["Nombre_Archivo"] = file  

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
        # Preparar para inserción masiva
        df = df.where(pd.notnull(df), None)  # <-- Esto evita el error
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO viajes VALUES ({placeholders})"
        # ejecuta por batch
        batch_size = 5000
        for start in range(0, len(values), batch_size):
            end = start + batch_size
            cursor.executemany(insert_query, values[start:end])
            connection.commit()
        #cursor.executemany(insert_query, values)
        #connection.commit()

        print(f"✅ Insertados {len(df)} registros desde {file}")

cursor.close()
connection.close()
print("🎉 Carga finalizada")
