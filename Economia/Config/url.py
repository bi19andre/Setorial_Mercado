



url_selic_meta = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?Format=json'
url_selic_realizada = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados?Format=json'

url_ipca_meta = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.13521/dados?formato=json'
url_ipca_previsto = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=10&$filter=Indicador%20eq%20%27IPCA%27&$orderby=Data%20desc&$format=text/html&$select=Data,DataReferencia,Media,baseCalculo"
url_ipca_realizado = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.13522/dados?Format=json'

url_pib = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=10&$filter=Indicador%20eq%20'PIB%20Total'&$orderby=Data%20desc&$format=text/html&$select=Data,DataReferencia,Mediana,baseCalculo"

url_pnad = "https://apisidra.ibge.gov.br/values/t/4093/n1/all/n3/all/v/4088,4092/p/last%201/c2/6794"