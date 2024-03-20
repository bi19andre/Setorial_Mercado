import pyodbc
import msal 


# Credenciais SQL

sql_server = ''
sql_database = ''
sql_username = ''
sql_password = ''

# Cadeias de conexão SQL

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + sql_server + ';DATABASE=' + sql_database + ';UID=' + sql_username + ';PWD=' + sql_password)
cursor = conn.cursor()


# Credenciais Power BI

pbi_username = ''
pbi_password = ''
app_id = ''
tenant_id = ''
authority_url = '' + tenant_id
scopes = ['']

#### Obter o token de acesso do Power BI

client = msal.PublicClientApplication(app_id, authority=authority_url)
response = client.acquire_token_by_username_password(username=pbi_username, password=pbi_password, scopes=scopes)
access_token = response.get('access_token')

#### Configuração do cabeçalho com o token de acesso

PBIheaders = {
    'Authorization': f'Bearer {access_token}'
}


#Power BI Análise Setorial
ws_setorial = ''
ds_setorial = ''
