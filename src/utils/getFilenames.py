import pandas as pd
import numpy as np



# funciones
def get_filnames_from_db(tabla, db):
    """
    Obtiene valores únicos del campo "Nombre_Archivo"
    """
    query = f"""SELECT DISTINCT Nombre_Archivo FROM {tabla};"""
    df = db.execute_query(query)
    # extraemos lista
    filenames = df['Nombre_Archivo'].unique().tolist()
    return filenames