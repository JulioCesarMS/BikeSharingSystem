import pandas as pd
from config.settings import GBFS_URL
import requests


def download_csv(url):
    return pd.read_csv(url, low_memory=False)



# descarga de estaciones
def download_stations():
    # selección campos
    col_sel = ["station_id", "short_name", "name","lat","lon","capacity"]
    # request
    response = requests.get(GBFS_URL)
    response.raise_for_status()
    data = response.json()
    stations = data["data"]["stations"]
    # dataframe
    df = pd.DataFrame(stations)
    df = df[col_sel]
    df = df.rename(columns={'short_name': 'cve_station'})
    return df
    