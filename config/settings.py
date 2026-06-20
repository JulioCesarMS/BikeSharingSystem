import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://ecobici.cdmx.gob.mx/"
DATA_URL = "https://ecobici.cdmx.gob.mx/en/open-data/"
GBFS_URL = "https://gbfs.mex.lyftbikes.com/gbfs/es/station_information.json"

DATA_PATH = "data/raw/"

# configuración base de datos
class DB:
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    NAME = os.getenv("DB_NAME")
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    
MYSQL_CONFIG = {
    "database": "ecobicis"
}