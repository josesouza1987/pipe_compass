#==================================================
# TÍTULO: extract_api.py
# DESCRIÇÃO: Nesse arquivo serão realizadas as chamadas de APIs que serão extraídas e transformadas.
#==================================================

import os
import pandas as pd
import requests as rq
from datetime import datetime as dt
from datetime import timedelta

class extractAPI():
    def __init__(self):
        # Declarando variáveis de data pra reutilizar
        self.dataInicial=dt.strftime(dt.today() - timedelta(days=365), '%Y-%m-%d')
        self.dataFinal=dt.now().strftime('%Y-%m-%d')

    def saveParquet(self, df, pasta):
        # Loop por ano e mês, no final vai salvar na estrutura de pastas /ano/mes por exemplo: /2025/08
        for (year, month), group in df.groupby(['year', 'month']):
            # Pasta do ano com formato year-YYYY
            year_path = os.path.join(pasta, f'year-{year}')
            os.makedirs(year_path, exist_ok=True)
            
            # Caminho do arquivo: month-MM.parquet
            file_path = os.path.join(year_path, f'month-{month:02d}.parquet')
            
            # Salvar Parquet removendo coluna index do dataframe
            group.drop(['year', 'month'], axis=1).to_parquet(file_path, engine='pyarrow', index=False)

    def updateSelic(self, dataInicial=None, dataFinal=None):
        # Caso não seja informado parâmetro de data será carregado a que foi definida na função __init___
        if dataInicial is None:
            dataInicial = self.dataInicial
        if dataFinal is None:
            dataFinal = self.dataFinal

        # Converte datas para o padrão brasileiro da API
        dataInicial = dt.strptime(dataInicial, '%Y-%m-%d').strftime('%d/%m/%Y')
        dataFinal = dt.strptime(dataFinal, '%Y-%m-%d').strftime('%d/%m/%Y')

        # Definindo parâmetros da API
        url = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados'
        params = {'dataInicial': dataInicial,'dataFinal': dataFinal}

        res = rq.get(url, params=params, timeout=30)
        res.raise_for_status()
        dados = res.json()

        df = pd.DataFrame(dados)

        # Transforma o campo data pra padrão americano yyyy-mm-dd
        df["data"] = pd.to_datetime(df["data"], dayfirst=True) # parâmetro dayfirst=True diz que o formato da data está no padrão brasileiro (dd/mm/yyyy)

        # Transformando o valor para para tipo float
        df["taxa_diaria_percentual"] = df["valor"].astype(float)

        # Realizando a multiplicação de 252 (dias de negociação) para dados relacionados ao mercado conforme a fonte https://www.investopedia.com/terms/a/annualize.asp
        # Nesse primeiro comando estou dividindo por 100 pra realizar o calculo de exponenciações
        df["taxa_diaria_decimal"] = df["taxa_diaria_percentual"] / 100.0

        # Anualização por 252 dias úteis conforme a documentação da fonte
        df["taxa_selic"] = ((1 + df["taxa_diaria_decimal"]) ** 252 - 1) * 100

        # Selecionando somente as colunas data e taxa_selic
        df = df[['data', 'taxa_selic']]

        # Criar colunas de ano e mês pra separar por arquivos e pastas
        df['year'] = df['data'].dt.year
        df['month'] = df['data'].dt.month
        
        # Criando o caminho da pasta base pra salvar arquivos
        pasta =  os.getenv('output') + 'dados_bruto/taxa_selic'

        # Renomeia a coluna "data" para "date" pra pradronizar
        df = df.rename(columns={"data": "date"})

        # Chama a função com resultado do dataframe pra gerar os arquivos em parquet
        self.saveParquet(df, pasta)

        print('Concluída a atualização dos dados da taxa Selic de ' + dataInicial + ' até ' + dataFinal)

        # Retorna o dataframe com resultado dessa função
        return df

    def updateDolar(self, dataInicial=None, dataFinal=None):
        # Caso não seja informado parâmetro de data será carregado a que foi definida na função __init___
        if dataInicial is None:
            dataInicial = self.dataInicial
        if dataFinal is None:
            dataFinal = self.dataFinal
        
        # Definindo parâmetros da API
        url = 'https://api.exchangerate.host/timeframe'

        # Lendo a variavel de ambiente com chave de acesso à API
        key = os.getenv('exchangerate_key')

        # Definindo parâmetros com intervalo de 365 dias.
        params = {'access_key': key,  'start_date': dataInicial, 'end_date': dataFinal, 'source': 'USD', 'currencies': 'BRL'}

        # Faz a requisição da API e transforma em dicionário
        res = rq.get(url, params=params)
        res_dict = res.json()

        # Criando uma lista de dicionários onde .items() retorna pares de chave e valor do dicionário
        # Sendo a coluna date recebe a chave e USDBRL recebe o valor que está dentro do dicionário
        # Usando o loop for pra percorrer cada item do dicionário res_dict['quotes']
        list_dict = [{'date': date, 'USDBRL': value['USDBRL']} for date, value in res_dict['quotes'].items()]

        # Transforma em Dataframe do pandas
        df = pd.DataFrame(list_dict)

        # Converter a coluna de data para datetime
        df['date'] = pd.to_datetime(df['date'])

        # Criar colunas de ano e mês pra separar por arquivos e pastas
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        
        # Criando o caminho da pasta base pra salvar arquivos
        pasta =  os.getenv('output') + 'dados_bruto/cotacao_dolar'

        # Chama a função com resultado do dataframe pra gerar os arquivos em parquet
        self.saveParquet(df, pasta)

        print('Concluída a atualização dos dados de cotação do dolar de ' + dataInicial + ' até ' + dataFinal)

        # Retorna o dataframe com resultado dessa função
        return df
    
    def dadosCurados(self, dataInicial=None, dataFinal=None):
        # Caso não seja informado parâmetro de data será carregado a que foi definida na função __init___
        if dataInicial is None:
            dataInicial = self.dataInicial
        if dataFinal is None:
            dataFinal = self.dataFinal

        df_selic = self.updateSelic(dataInicial, dataFinal)
        df_dolar = self.updateDolar(dataInicial, dataFinal)

        # Faz o merge pela coluna "date" e mantendo todas as datas
        df_final = pd.merge(df_dolar, df_selic, on="date", how="outer")

        # Selecionando somente as colunas necessárias até o momento
        df_final = df_final[['date','USDBRL', 'taxa_selic']]

        # Recriando as colunas de ano e mês
        df_final['year'] = df_final['date'].dt.year
        df_final['month'] = df_final['date'].dt.month

        # Criando o caminho da pasta base pra salvar arquivos
        pasta =  os.getenv('output') + 'curado/dados_financeiros'

        # Chama a função com resultado do dataframe pra gerar os arquivos em parquet
        self.saveParquet(df_final, pasta)

        print('Concluída a atualização das bases bruto e curado')
