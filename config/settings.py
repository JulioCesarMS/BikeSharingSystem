import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://ecobici.cdmx.gob.mx/"
DATA_URL = "https://ecobici.cdmx.gob.mx/en/open-data/"

DATA_PATH = "data/raw/"

MYSQL_CONFIG = {
    "database": "ecobicis"
}