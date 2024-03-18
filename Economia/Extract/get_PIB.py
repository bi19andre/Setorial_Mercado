import pandas as pd
import requests

from bs4 import BeautifulSoup

from Economia.Config.utils import extract
from Config.sql import sqlIPCAmeta
from Economia.Config.url import url_pib

def get_pib():

    response = requests.get(url_pib)
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')

    data = []
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) == 4:  # Verificar se a linha contém os dados esperados
            data.append({
                'publicacao': pd.to_datetime(cells[0].text),
                'data': pd.to_datetime(cells[1].text),
                'valor': float(cells[2].text.replace('.', '').replace(',', '.')),
                'baseCalculo': int(cells[3].text)
            })

    df = pd.DataFrame(data)
    df = df.loc[df['baseCalculo'] == 0]
    df = df.drop(columns=['baseCalculo'])
    df['valor'] = round(df['valor'].astype(float) / 100, 3)
    df['natureza'] = 'Previsto'
    df = df.reindex(columns=['data', 'natureza', 'valor', 'publicacao'])

    max = df['publicacao'].max()
    df = df.loc[df['publicacao'] == max].reset_index(drop=True)

    return df