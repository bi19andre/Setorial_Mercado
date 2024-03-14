
import pandas as pd
import sys
import warnings
import os

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.append('/apoiodev/')
from connect import conn, command_bcp, gmaps
from config import cd_Bcp, cd_CacheGeo
warnings.filterwarnings("ignore")

class Geocoder:
    def __init__(self, df):
        self.df = df


    @staticmethod
    def geocodificar(endereco):
        try:
            geocode_result = gmaps.geocode(endereco)
            if geocode_result:
                resultado = geocode_result[0]
                dados = {
                    'endereco_completo': resultado.get('formatted_address', ''),
                    'latitude': resultado['geometry']['location'].get('lat', ''),
                    'longitude': resultado['geometry']['location'].get('lng', ''),
                    'bairro': '',
                    'cidade': '',
                    'uf': '',
                    'cep': ''
                }
                for component in resultado['address_components']:
                    if 'sublocality_level_1' in component['types']:
                        dados['bairro'] = component['long_name']
                    if 'administrative_area_level_2' in component['types']:
                        dados['cidade'] = component['long_name']
                    if 'administrative_area_level_1' in component['types']:
                        dados['uf'] = component['short_name']
                    if 'postal_code' in component['types']:
                        dados['cep'] = component['long_name']
                return dados
        except Exception as e:
            print(f'Erro ao geocodificar {endereco}: {e}')
            return None   

    def Processar (self):

        enderecos = self.df['endereco'].tolist()
        resultados = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_endereco = {executor.submit(self.geocodificar, endereco): endereco for endereco in enderecos}
            for future in as_completed(future_to_endereco):
                resultado = future.result()
                if resultado:
                    idx = enderecos.index(future_to_endereco[future])
                    self.df.at[idx, 'address'] = resultado['endereco_completo']
                    self.df.at[idx, 'latitude'] = resultado['latitude']
                    self.df.at[idx, 'longitude'] = resultado['longitude']
                    self.df.at[idx, 'bairro'] = resultado['bairro']
                    self.df.at[idx, 'cidade'] = resultado['cidade']
                    self.df.at[idx, 'uf'] = resultado['uf']
                    self.df.at[idx, 'cep'] = resultado['cep']

        self.df.to_csv(f"{cd_CacheGeo}Geocode_{data_atual.strftime('%Y_%m_%d_%H_%M')}.csv")
       
        return self.df