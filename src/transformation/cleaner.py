import pandas as pd
import numpy as np
    


def parse_fecha(col):
    col = col.astype(str)

    fecha = pd.to_datetime(col, format="%Y-%m-%d", errors="coerce")
    mask = fecha.isna()
    fecha.loc[mask] = pd.to_datetime(col[mask], format="%d/%m/%Y", errors="coerce")

    return fecha


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
    #df = df.replace(["NULL", "null", "None"], np.nan)
    # Convertir tipos de columnas (manejar errores → NaN)
    df["Edad_Usuario"] = pd.to_numeric(df["Edad_Usuario"], errors="coerce")
    df["Bici"] = pd.to_numeric(df["Bici"], errors="coerce")
    
    #df["Ciclo_Estacion_Retiro"] = df["Ciclo_Estacion_Retiro"].astype(str).str.split("-").str[0]
    #df["Ciclo_Estacion_Retiro"] = df["Ciclo_Estacion_Retiro"].apply(lambda x: int(x) if pd.notna(x) else None)
    #df["Ciclo_Estacion_Arribo"] = df["Ciclo_Estacion_Arribo"].astype(str).str.split("-").str[0]
    #df["Ciclo_Estacion_Arribo"] = df["Ciclo_Estacion_Arribo"].apply(lambda x: int(x) if pd.notna(x) else None)

    df["Ciclo_Estacion_Retiro"] = (
        df["Ciclo_Estacion_Retiro"]
        .astype(str)
        .str.split("-").str[0]
        .replace("nan", np.nan)  
    )

    df["Ciclo_Estacion_Retiro"] = pd.to_numeric(df["Ciclo_Estacion_Retiro"], errors="coerce")
    
    
    df["Ciclo_Estacion_Arribo"] = (
        df["Ciclo_Estacion_Arribo"]
        .astype(str)
        .str.split("-").str[0]
        .replace("nan", np.nan)
    )

    df["Ciclo_Estacion_Arribo"] = pd.to_numeric(df["Ciclo_Estacion_Arribo"], errors="coerce")
    # df["Fecha_Retiro"] = pd.to_datetime(df["Fecha_Retiro"], errors="coerce", dayfirst=True).dt.date
    # df["Fecha_Arribo"] = pd.to_datetime(df["Fecha_Arribo"], errors="coerce", dayfirst=True).dt.date
    # df["Hora_Retiro"] = pd.to_datetime(df["Hora_Retiro"], errors="coerce", format="%H:%M:%S").dt.time
    # df["Hora_Arribo"] = pd.to_datetime(df["Hora_Arribo"], errors="coerce", format="%H:%M:%S").dt.time
    # Tomar primeros 10 caracteres de las fechas
    # try:   
    #     df["Fecha_Retiro"] = pd.to_datetime(df["Fecha_Retiro"].astype(str), dayfirst=True, errors="coerce").dt.date
    # except:
    #     df["Fecha_Retiro"] = pd.to_datetime(df["Fecha_Retiro"].astype(str), dayfirst=False, errors="coerce").dt.date
    
    # try:
    #     df["Fecha_Arribo"] = pd.to_datetime(df["Fecha_Arribo"].astype(str), dayfirst=True, errors="coerce").dt.date
    # except:
    #     df["Fecha_Arribo"] = pd.to_datetime(df["Fecha_Arribo"].astype(str), dayfirst=False, errors="coerce").dt.date

    df["Fecha_Retiro"] = parse_fecha(df["Fecha_Retiro"])
    df["Fecha_Arribo"] = parse_fecha(df["Fecha_Arribo"])
    df["Fecha_Retiro"] = df["Fecha_Retiro"].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
    df["Fecha_Arribo"] = df["Fecha_Arribo"].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
    # Limpiar horas 12h AM/PM, cambiar . por : si viene 12:01.59 a.m.
    df["Hora_Retiro"] = df["Hora_Retiro"].astype(str).str[:8]
    df["Hora_Retiro"] = pd.to_datetime(
        df["Hora_Retiro"].str.replace(r'\.', ':', regex=True),
        format="%H:%M:%S",
        errors="coerce"
    )
    df["Hora_Arribo"] = df["Hora_Arribo"].astype(str).str[:8]
    df["Hora_Arribo"] = pd.to_datetime(
        df["Hora_Arribo"].str.replace(r'\.', ':', regex=True),
        format="%H:%M:%S",
        errors="coerce"
    )
    
    df["Hora_Arribo"] = df["Hora_Arribo"].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
    df["Hora_Retiro"] = df["Hora_Retiro"].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
    
    df = df.astype(object)
    df = df.where(pd.notnull(df), None)
    
    for col in ["Fecha_Retiro", "Fecha_Arribo", "Hora_Retiro", "Hora_Arribo"]:
        print(col, df[col].apply(type).unique())
    return df