import os 
import re
import subprocess
import pyodbc
import requests
import json
import pandas as pd 

from .connect import *


def command_bcp (database, schema, table, path):
    # Executa o comando bulk insert para população da base de dados em grande escala e menor tempo.        
    bcp_string = f'bcp [{database}].[{schema}].[{table}] in "' + os.path.abspath(path) + '" -c -C 65001 -U "' + sql_username + '" -S tcp:' + sql_server + ' -P "' + sql_password + '" -F 2 -t ";"'  
    process = subprocess.Popen(bcp_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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


def transformar_em_data(data_str):
    match = re.match(r"(\d{4})(\d{2})", data_str)
    ano, mes = match.groups()      
    data_formatada = f"{ano}-{mes}-01"
    return data_formatada