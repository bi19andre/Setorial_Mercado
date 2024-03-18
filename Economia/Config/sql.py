
import pandas as pd 
from .connect import conn

sqlSelic = pd.read_sql("SELECT MAX(data) FROM setorial_f_selic;", conn)
sqlIPCAmeta = pd.read_sql("SELECT MAX(data) FROM setorial_f_ipca_meta;", conn)
sql_desemprego = pd.read_sql("SELECT DISTINCT iddata FROM setorial_f_desemprego;", conn)

delete_ipca_meta = f"DELETE FROM setorial_f_ipca_meta WHERE YEAR(data) >= {Ano};"
delete_ipca_previsto = "DELETE FROM setorial_f_ipca_previsto;"
delete_ipca = "DELETE FROM setorial_f_ipca_realizado;"
delete_pib = "DELETE FROM setorial_f_pib WHERE natureza = 'previsto';"
