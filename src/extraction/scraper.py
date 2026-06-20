import requests
from bs4 import BeautifulSoup
from config.settings import DATA_URL, BASE_URL

def get_file_links():
    result = requests.get(DATA_URL)
    soup = BeautifulSoup(result.text, "lxml")
      
    links = []

    box = soup.find('div', class_='elementor-container')

    for anio in box.find_all('div', class_='elementor-toggle-item'):
        lista = anio.find('ul')
        for mes in lista.find_all('a', href=True):
            full_url = BASE_URL + mes['href']
            filename = mes['href'].split("/")[-1]

            links.append({
                "url": full_url,
                "filename": filename.replace(".csv", "")
            })

    return links