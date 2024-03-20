from Config.utils import command_bcp
from datetime import datetime

def insert(df, schema, tabela, bcp_path):
    if not tabela.empty():
        now = datetime.now().strftime("%Y_%m_%d")
        bcp_file = f"{bcp_path}{tabela}{now}.csv"
        df.to_csv(bcp_file, sep=';', encoding='utf-8', index=False)
        print(f"{len(tabela)} linhas exportadas para csv")
        result = command_bcp(schema=schema, table=tabela, bcp_file=bcp_file) 
        print(result)
    return result