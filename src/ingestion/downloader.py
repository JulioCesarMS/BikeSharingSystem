import pandas as pd

def download_csv(url):
    return pd.read_csv(url, low_memory=False)