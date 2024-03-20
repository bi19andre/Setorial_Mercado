import pandas as pd
import requests

from bs4 import BeautifulSoup

from Config.utils import extract
from Config.url import url_ipca_meta, url_ipca_previsto, url_ipca_realizado


def get_ipca (ano):

    ###### IPCA META

    ipca_meta = extract(url_ipca_meta)
    ipca_meta['data'] = pd.to_datetime(ipca_meta['data'], format='%Y/%m/%d')
    ipca_meta['valor'] = ipca_meta['valor'].astype(float)
    ipca_meta = ipca_meta.loc[ipca_meta['data'].dt.year >= ano]

    ###### IPCA PREVISTO

    response = requests.get(url_ipca_previsto)
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

    ipca_previsto = pd.DataFrame(data)
    ipca_previsto = ipca_previsto.loc[ipca_previsto['baseCalculo'] == 1]
    ipca_previsto = ipca_previsto.drop(columns=['baseCalculo'])
    ipca_previsto['valor'] = ipca_previsto['valor'].astype(float)

    ###### IPCA REALIZADO

    ipca_realizado = extract(url_ipca_realizado)
    ipca_realizado['data'] = pd.to_datetime(ipca_realizado['data'])
    max = ipca_realizado['data'].max()
    ipca_realizado = ipca_realizado.loc[(ipca_realizado['data'].dt.month == 12) | (ipca_realizado['data'] == max)].reset_index(drop=True)
    ipca_realizado['data'] = pd.to_datetime(ipca_realizado['data'].dt.year, format='%Y')
    ipca_realizado['data'] = pd.to_datetime(ipca_realizado['data'], format='%Y/%m/%d')
    ipca_realizado['valor'] = ipca_realizado['valor'].astype(float)


    return ipca_meta, ipca_previsto, ipca_realizado


