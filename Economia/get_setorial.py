
###############################################################
###### Web Scraping para atualização da Análise Setorial ######
###############################################################

#####################
###### Connect ######
#####################

import pandas as pd
import requests
import json
import datetime
import pyodbc
import warnings
import pyodbc 
import msal 
import os
import re
import subprocess

from bs4 import BeautifulSoup
from datetime import datetime, date

warnings.filterwarnings("ignore")

cd_Bcp = '/OneDrive - Apoio/0.pyData/bcp/'



#### CONNECT ####

server = 'SERVER'
database = 'DATABASE'
sql_username = 'USER'
sql_password = "PASS"
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + sql_username + ';PWD=' + sql_password)
cursor = conn.cursor()


username = 'USER'
password = "PAS"
app_id = 'XXXX-XXX-XXXXX'
tenant_id = 'XXXX-XXX-XXXXX'
authority_url = 'https://login.microsoftonline.com/' + tenant_id
scopes = ['https://analysis.windows.net/powerbi/api/.default']
#### Obter o token de acesso do Power BI
client = msal.PublicClientApplication(app_id, authority=authority_url)
response = client.acquire_token_by_username_password(username=username, password=password, scopes=scopes)
access_token = response.get('access_token')
#### Configuração do cabeçalho com o token de acesso
PBIheaders = {
    'Authorization': f'Bearer {access_token}'
}

#Power BI Análise Setorial
ws_setorial = 'XXXX-XXX-XXXXX'
ds_setorial = 'XXXX-XXX-XXXXX'

# FUNÇÕES

def command_bcp (schema, table, bcp_file):
    
    command = f'bcp [DATABASE].[{schema}].[{table}] in "' + os.path.abspath(bcp_file) + '" -c -C 65001 -U "' + sql_username + '" -S tcp:SERVER -P "' + sql_password + '" -F 2 -t ";"'    

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, _ = process.communicate()
    padrao = r'\n(\d+) linhas copiadas.'
    numeros = re.findall(padrao, output.decode('latin-1'))
    numeros = numeros[0] if numeros else ''
    result = f'{numeros} linhas copiadas para o SQL Server'
    return result

def execute_command (command, values=None): 
    cursor = conn.cursor()
    try:
        if values:
            cursor.execute(command, values)
        else:
            cursor.execute(command)
        conn.commit()
        resultado_command = cursor.rowcount
        return resultado_command
    except pyodbc.Error as e:
        conn.rollback()
        print(f"Erro ao executar o comando SQL: {str(e)}")

def update_pbi (ws, ds):
    url_pbi = f'https://api.powerbi.com/v1.0/myorg/groups/{ws}/datasets/{ds}/refreshes'
    response = requests.post(url_pbi, headers=PBIheaders)
    if response.status_code == 202:
        MsgPBI = 'Solicitação de atualização enviada com sucesso.'
    else:
        MsgPBI = 'Ocorreu um erro ao enviar a solicitação de atualização.'
        MsgPBI = 'Código de status:', response.status_code
        MsgPBI = 'Resposta:', response.json()
    return MsgPBI



def extract(url):
    response = requests.get(url)
    data = json.loads(response.content)
    df = pd.DataFrame(data, columns=['data', 'valor'])
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
    df = df.loc[df['data'] >= pd.to_datetime('2018-01-01')]    
    return df

# Variáveis Básicas

Atual = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
Ano = date.today().year
print('')
print(f'{Atual}: Buscando dados...')
print('')

# Parâmetros SQL

sqlSelic = pd.read_sql("SELECT MAX(data) FROM setorial_f_selic;", conn)
sqlIPCAmeta = pd.read_sql("SELECT MAX(data) FROM setorial_f_ipca_meta;", conn)
sql_desemprego = pd.read_sql("SELECT DISTINCT iddata FROM setorial_f_desemprego;", conn)



########################
###### TAXA SELIC ######
########################

# Extract SELIC meta
url_selic_meta = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?Format=json'
selic_meta = extract(url_selic_meta)
selic_meta = selic_meta.rename(columns = {'valor': 'meta'})

# Extract SELIC realizada
url_selic_realizada = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?Format=json'
selic_realizada = extract(url_selic_realizada)
selic_realizada = selic_realizada.rename(columns={'valor': 'selic'})
selic_realizada_dict = selic_realizada.set_index('data')['selic'].to_dict()

selic = selic_meta.copy()
selic['selic'] = selic['data'].map(selic_realizada_dict)
selic['selic'] = selic['selic'].fillna(0)
selic = selic.loc[selic['data'] > sqlSelic.iloc[0,0]]
selic = selic.reset_index(drop=True)
selic['meta'] = selic['meta'].astype(float)
selic['selic'] = selic['selic'].astype(float)

if len(selic) > 0:
    print(f'SELIC: {len(selic)} linhas copiadas para área de transferência')
    for _, row in selic.iterrows():
        try:
            insert_selic = f"INSERT INTO setorial_f_selic (data, meta, selic) VALUES (?, ?, ?);"
            values_selic = (row['data'], row['meta'], row['selic'])
            resultado_insert_selic = execute_command(insert_selic, values_selic)

        except pyodbc.DatabaseError as e:
            conn.rollback()
            print(f"Erro ao inserir registro {row}: {str(e)}")
        else:
            conn.commit()
else:
    print("SELIC: dados novos indisponíveis na fonte")




#######################
###### IPCA META ######
#######################

url_ipca_meta = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.13521/dados?formato=json'
ipca_meta = extract(url_ipca_meta)
ipca_meta['data'] = pd.to_datetime(ipca_meta['data'], format='%Y/%m/%d')
ipca_meta['valor'] = ipca_meta['valor'].astype(float)
ipca_meta = ipca_meta.loc[ipca_meta['data'].dt.year >= Ano]

if len(ipca_meta) > 0:

    delete_ipca_meta = f"DELETE FROM setorial_f_ipca_meta WHERE YEAR(data) >= {Ano};"
    resultado_delete_ipca_meta = execute_command(delete_ipca_meta)

    print(f'IPCA Meta: {len(ipca_meta)} linhas copiadas para área de transferência')

    for _, row in ipca_meta.iterrows():
        try:
            insert_ipca_meta = f"INSERT INTO setorial_f_ipca_meta (data, valor) VALUES (?, ?);"
            values_ipca_meta = (row['data'], row['valor'])
            resultado_insert_ipca_meta = execute_command(insert_ipca_meta, values_ipca_meta)

        except pyodbc.DatabaseError as e:
            conn.rollback()
            print(f"Erro ao inserir registro {row}: {str(e)}")
        else:
            conn.commit()
else:
    print("IPCA Meta: novos ")




###########################
###### IPCA PREVISTO ######
###########################

url_ipca_previsto = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=10&$filter=Indicador%20eq%20%27IPCA%27&$orderby=Data%20desc&$format=text/html&$select=Data,DataReferencia,Media,baseCalculo"

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

if len(ipca_previsto) > 0:

    delete_ipca_previsto = "DELETE FROM setorial_f_ipca_previsto;"
    resultado_delete_ipca_previsto = execute_command(delete_ipca_previsto)

    print(f'IPCA Previsto: {len(ipca_previsto)} linhas copiadas para área de transferência')
    for _, row in ipca_previsto.iterrows():
        try:
            insert_ipca_previsto = f"INSERT INTO setorial_f_ipca_previsto (publicacao, data, valor) VALUES (?, ?, ?);"
            values_ipca_previsto = (row['publicacao'], row['data'], row['valor'])
            resultado_insert_ipca_previsto = execute_command(insert_ipca_previsto, values_ipca_previsto)

        except pyodbc.DatabaseError as e:
            conn.rollback()
            print(f"Erro ao inserir registro {row}: {str(e)}")
        else:
            conn.commit()
else:
    print("IPCA Previsto: novos ")




############################
###### IPCA REALIZADO ######
############################

url_ipca = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados?Format=json'
ipca = extract(url_ipca)
ipca['data'] = pd.to_datetime(ipca['data'])
max = ipca['data'].max()
ipca = ipca.loc[(ipca['data'].dt.month == 12) | (ipca['data'] == max)].reset_index(drop=True)
ipca['data'] = pd.to_datetime(ipca['data'].dt.year, format='%Y')
ipca['data'] = pd.to_datetime(ipca['data'], format='%Y/%m/%d')
ipca['valor'] = ipca['valor'].astype(float)
if len(ipca) > 0:

    delete_ipca = "DELETE FROM setorial_f_ipca;"
    resultado_delete_ipca = execute_command(delete_ipca)

    print(f'IPCA Realizado: {len(ipca)} linhas copiadas para área de transferência')

    if len(ipca) > 0:
        for _, row in ipca.iterrows():
            try:
                insert_ipca = f"INSERT INTO setorial_f_ipca (data, valor) VALUES (?, ?);"
                values_ipca = (row['data'], row['valor'])
                resultado_insert_ipca = execute_command(insert_ipca, values_ipca)

            except pyodbc.DatabaseError as e:
                conn.rollback()
                print(f"Erro ao inserir registro {row}: {str(e)}")
            else:
                conn.commit()
else:
    print("IPCA Realizado: novos ")




#################
###### PIB ######
#################

url_pib = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=10&$filter=Indicador%20eq%20'PIB%20Total'&$orderby=Data%20desc&$format=text/html&$select=Data,DataReferencia,Mediana,baseCalculo"

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

pib = pd.DataFrame(data)
pib = pib.loc[pib['baseCalculo'] == 0]
pib = pib.drop(columns=['baseCalculo'])
pib['valor'] = round(pib['valor'].astype(float) / 100, 3)
pib['natureza'] = 'Previsto'
pib = pib.reindex(columns=['data', 'natureza', 'valor', 'publicacao'])

max = pib['publicacao'].max()
pib = pib.loc[pib['publicacao'] == max].reset_index(drop=True)

if len(pib) > 0:
    delete_pib = "DELETE FROM setorial_f_pib WHERE natureza = 'previsto';"
    resultado_delete_pib = execute_command(delete_pib)

    print(f'PIB: {len(pib)} linhas copiadas para área de transferência')
    for _, row in pib.iterrows():
        try:
            insert_pib = "INSERT INTO setorial_f_pib (iddata, natureza, valor, atualizacao) VALUES (?, ?, ?, ?)"
            values_pib = (row['data'], row['natureza'], row['valor'], row['publicacao'])
            resultado_insert_pib = execute_command(insert_pib, values_pib)

        except pyodbc.DatabaseError as e:
            conn.rollback()
            print(f"Erro ao inserir registro {row}: {str(e)}")
        else:
            conn.commit()
else:
    print("PIB: novos ")





########################################
###### Desemprego Trimestral IBGE ######
########################################
    

desemprego_iddata_list = list(sql_desemprego['iddata'].unique())

url = "https://apisidra.ibge.gov.br/values/t/4093/n1/all/n3/all/v/4088,4092/p/last%201/c2/6794"
response = requests.get(url)
if response.status_code == 200:
    dados = response.json()
    df = pd.DataFrame(dados)
    if 'NC' in df.columns:  
        df = df[1:]     
else:
    print("Falha na requisição à API. Status code:", response.status_code)


def transformar_em_data(data_str):
    match = re.match(r"(\d{4})(\d{2})", data_str)
    ano, mes = match.groups()      
    data_formatada = f"{ano}-{mes}-01"
    return data_formatada


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
saida_desemprego = base_desemprego.loc[~base_desemprego['iddata'].isin(desemprego_iddata_list)].reset_index(drop=True).copy()

if not saida_desemprego.empty:
    saida_desemprego.to_csv
    bcp_file = f"{cd_Bcp}desemprego_{df['D3C'].iloc[0]}.csv"
    saida_desemprego.to_csv(bcp_file, sep=';', encoding='utf-8', index=False)
    result = command_bcp(schema='dbo', table='setorial_f_desemprego', bcp_file=bcp_file) 
    print("______________________________________________________________")
    print(f"PNAD Contínua IBGE - Desemprego Trimestral: {result}")
else:
    print("PNAD Contínua IBGE - Desemprego Trimestral: sem atualizações disponíveis")



# ATUALIZA DS

update_url = f'https://api.powerbi.com/v1.0/myorg/groups/{ws_setorial}/datasets/{ds_setorial}/refreshes'
response = requests.post(update_url, headers=PBIheaders)

if response.status_code == 202:
    MsgPBI = 'DS Análise Setorial - Solicitação de atualização enviada com sucesso.'
else:
    MsgPBI = 'DS Análise Setorial - Ocorreu um erro ao enviar a solicitação de atualização.'
    MsgPBI = 'DS Análise Setorial - Código de status:', response.status_code
    MsgPBI = 'DS Análise Setorial - Resposta:', response.json()

print('')
print(MsgPBI)
print('')


input("Pressione Enter para sair...")