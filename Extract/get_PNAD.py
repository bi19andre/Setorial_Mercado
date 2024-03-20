import pandas as pd 
import requests
from Config.url import url_pnad
from Config.utils import transformar_em_data
from Config.connect import conn


def get_pnad ():

    response = requests.get(url_pnad)
    if response.status_code == 200:
        dados = response.json()
        df = pd.DataFrame(dados)
        if 'NC' in df.columns:  
            df = df[1:]     
    else:
        print("Falha na requisição à API. Status code:", response.status_code)


    def separa_tabela(df, pattern):
        df = df.rename(columns={'D1C': 'idibge', 'D3N': 'periodo', 'V': 'valor'})
        df = df.loc[df['D2N'].str.contains(pattern)].reset_index(drop=True)
        df.loc[df['idibge'] == '1', 'idibge'] = '76'
        df = df[['idibge', 'D3C', 'periodo', 'valor']]
        df['iddata'] = pd.to_datetime(df['D3C'].apply(lambda x: transformar_em_data(x)))
        df['periodo'] = df['periodo'].astype(str)
        df['periodo'] = df['periodo'].str.replace('trimestre', 'Tri')
        df = df[['iddata', 'idibge', 'periodo', 'valor']]
        df['valor'] = df['valor'].astype(int)
        df['idibge'] = df['idibge'].astype(int)
        return df

    base_desemprego = separa_tabela(df, 'na força')
    desocupados = separa_tabela(df, 'desocupadas')

    for idx, row in desocupados.iterrows():
        iddata = row['iddata']
        idibge = row['idibge']
        periodo = row['periodo']
        valor = row['valor']
        base_desemprego.loc[
            (base_desemprego['iddata'] == iddata) &
            (base_desemprego['idibge'] == idibge) &
            (base_desemprego['periodo'] == periodo), 'valor2'] = valor 

    base_desemprego['percentual'] = round(base_desemprego['valor2'] / base_desemprego['valor'], 3)
    base_desemprego = base_desemprego[['iddata', 'idibge', 'periodo', 'percentual']]

    # Se a tabela setorial_f_desemprego já existe, eliminamos os dados já populados na base para evitar duplicidades.
    # sql_desemprego = pd.read_sql("SELECT DISTINCT iddata FROM setorial_f_desemprego;", conn)
    # desemprego_iddata_list = list(sql_desemprego['iddata'].unique())
    # df = base_desemprego.loc[~base_desemprego['iddata'].isin(desemprego_iddata_list)].reset_index(drop=True).copy()

    return df