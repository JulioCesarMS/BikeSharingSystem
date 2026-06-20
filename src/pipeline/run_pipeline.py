from src.extraction.scraper import get_file_links
from src.extraction.downloader import download_csv, download_stations
from src.transformation.cleaner import transform_data_staging, transform_dim_station
from src.load.loader import load_data
from src.database.mysql_client import MySQLDatabase
from src.utils.getFilenames import get_filnames_from_db
from src.database.create_staging import table_staging
from src.database.create_staging import CreateTables
from src.database.create_star_model import create_dim_station
import warnings
import traceback
import time
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")



def run():
    
    print("--------------------------------------------")
    print("               Base de Ecobicis             ")
    print("--------------------------------------------")
    
    start_time = time.time()
    # conexión a base de datos
    #db = MySQLDatabase("ecobicis")
    print("\n---      Creación de tablas       ---")
    db = CreateTables("ecobicis")
    print("Tabla staging creada con éxito")
    create_dim_station(db)
    print("Tabla dim_station creada con éxito")
    # tabal a actualizar
    table_name = "staging_viajes"
    # creat tabla staging_viajes sino existe
    db.staging(table_name)
    # verificamos cuantos archivos se cargaron dn base de datos
    dbfilenames = get_filnames_from_db(table_name, db)
    print(f'✅Total archivos en bd: {len(dbfilenames)}')
    
    # links meses a descargar
    links = get_file_links()
    # total archivos 
    print(f"🔎 Archivos encontrados: {len(links)}\n")
    
    for item in links:
        
        filename = item["filename"]
        url = item["url"]
        # agregamos a la lista de cargados
        if (filename in dbfilenames):
            print(f" Ya procesado: {filename}")
            continue

        print(f"📥 Descargando: {filename}")

        try:
            # descarga de datos
            raw = download_csv(url)
            # limpieza de datos
            df_staging = transform_data_staging(raw, filename)
            # inserta datos en batch a Mysql
            load_data(db, df_staging, table_name)
            #db.insertar_to_db(df, "viajes", batch_size=5000)
            # agregamos archivo cargado a la lista
            dbfilenames.append(filename)
            #save_processed(filename)
            # imprime nombre archivo y tanmaño
            print(f"✅ Cargado: {filename} ({len(df_staging)} registros)")

        except Exception as e:
            traceback.print_exc()
            print(f"❌ Error en {filename}: {e}") 
      
    # carga de dimensiones
    print(f"📥 Descargando: dim_station")
    df_stations = download_stations()
    df_transform_stations = transform_dim_station(df_stations)
    load_data(db, df_transform_stations, table_name='dim_station')
    print(f"✅ Cargando: dim_station ({len(df_transform_stations)} registros)")
    
    
    end_time = time.time()
    duration = int((end_time - start_time)//60)
    print(f"Tiempo: {duration} minutos")
    
    db.close()
    
    
    
# if __name__ == "__main__":
#     run()