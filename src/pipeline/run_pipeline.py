from src.ingestion.scraper import get_file_links
from src.ingestion.downloader import download_csv
from src.transformation.cleaner import clean_data
from src.tracking.file_tracker import load_processed, save_processed
from src.database.mysql_client import MySQLDatabase
from src.utils.getFilenames import get_filnames_from_db
import warnings
import traceback
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")



def run():
    # conexión a base de datos
    db = MySQLDatabase("ecobicis")
    # tabal a actualizar
    tabla = "viajes"
    # verificamos cuantos archivos se cargaron dn base de datos
    dbfilenames = get_filnames_from_db(tabla, db)
    print(f'✅Total archivos en bd: {len(dbfilenames)}\n')
    
    
    # links meses a descargar
    links = get_file_links()
    # total archivos 
    print(f"🔎 Archivos encontrados: {len(links)}")

    for item in links:

        filename = item["filename"]
        url = item["url"]
        # carga archivos descargados
        processed = load_processed()
        if (filename in processed) or ((filename in dbfilenames)):
            print(f"⏭️ Ya procesado: {filename}")
            continue

        print(f"📥 Descargando: {filename}")

        try:
            # descarga de datos
            raw = download_csv(url)
            # limpieza de datos
            df = clean_data(raw, filename)
            # inserta datos en batch a Mysql
            db.insertar_to_db(df, "viajes", batch_size=5000)
            # guarda archivos procesados
            save_processed(filename)
            # imprime nombre archivo y tanmaño
            print(f"✅ Insertado: {filename} ({len(df)} registros)")



        except Exception as e:
            traceback.print_exc()
            print(f"❌ Error en {filename}: {e}")

    db.close()
    
    
    
# if __name__ == "__main__":
#     run()