from Config.utils import extract
from Config.sql import sqlSelic
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
    selic = selic.loc[df['data'] > sqlSelic.iloc[0,0]]
    selic = selic.reset_index(drop=True)
    df['meta'] = df['meta'].astype(float)
    df['selic'] = df['selic'].astype(float)

    return df