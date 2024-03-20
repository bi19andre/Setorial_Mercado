import pandas as pd
from Config.connect import conn
from Config.utils import extract
from Config.url import url_selic_meta, url_selic_realizada

def get_selic ():

    # Extract SELIC meta
    selic_meta = extract(url_selic_meta)
    selic_meta = selic_meta.rename(columns = {'valor': 'meta'})

    # Extract SELIC realizada
    selic_realizada = extract(url_selic_realizada)
    selic_realizada = selic_realizada.rename(columns={'valor': 'selic'})
    selic_realizada_dict = selic_realizada.set_index('data')['selic'].to_dict()

    df = selic_meta.copy()
    df['selic'] = df['data'].map(selic_realizada_dict)
    df['selic'] = df['selic'].fillna(0)

    # Como a tabela setorial_f_desemprego já existe, eliminamos os dados já populados na base para evitar duplicidades.
    #sqlSelic = pd.read_sql("SELECT MAX(data) FROM setorial_f_selic;", conn)
    #df = df.loc[df['data'] > sqlSelic.iloc[0,0]].reset_index(drop=True)
    
    df['meta'] = df['meta'].astype(float)
    df['selic'] = df['selic'].astype(float)

    return df