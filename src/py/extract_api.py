#==================================================
# TÍTULO: extract_api.py
# DESCRIÇÃO: Nesse arquivo serão realizadas as chamadas de APIs que serão extraídas e transformadas.
#==================================================

import os
import pandas as pd
import requests as rq
from datetime import datetime as dt
from datetime import timedelta
import time
import sys
from .func_aux import funcAux
aux = funcAux()

class extractAPI():
    def __init__(self):
        # Declarando variáveis de data pra reutilizar
        self.dataInicial=dt.strftime(dt.today() - timedelta(days=365), '%Y-%m-%d')
        self.dataFinal=dt.now().strftime('%Y-%m-%d')

        # Caminho da pasta de log, se não existe cria uma e variaveis de número de tentativas e tempo
        self.log_arquivo = os.getenv('output') + 'logs/logs_pipeline.csv'
        os.makedirs(os.path.dirname(self.log_arquivo), exist_ok=True)
        self.tentativas = 5
        self.tempo=10 # em segundos

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

        # Realiza a checagem se a resposta da API é 200 com sucesso se não faz 5 tentativas a cada 10 segundos
        # Também realiza o salvamento de mensagens de log em caso de erro
        for i in range(1, self.tentativas + 1):
            res = rq.get(url, params=params, timeout=30)
            if res.status_code == 200:
                break
            else:
                aux.log('Erro', 'API Selic do BCB na tentativa ' + str(i) + ' falhou com erro: ' + str(res.status_code))
                time.sleep(self.tempo)
                if i == 5:
                    sys.exit(1) # fecha o código imediatamente

        # Gera um log se ocorrer erro na transformação dos dados
        try:
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
            aux.saveParquet(df, pasta)

            # Gera log em caso de sucesso
            aux.log('Sucesso', 'Concluída a atualização dos dados da taxa Selic de ' + dataInicial + ' até ' + dataFinal)

            # Retorna o dataframe com resultado dessa função
            return df
            
        # Se ocorrer erro nos tratamentos dos dados gera log de erro e finalza programa
        except Exception as erro:
                    aux.log('Erro', 'API Selic do BCB falhou no tratamento de dados com erro: ' + str(erro))
                    exit(0) # Pra encessar a execução do console

    def updateDolar(self, dataInicial=None, dataFinal=None):
        # Caso não seja informado parâmetro de data será carregado a que foi definida na função __init___
        if dataInicial is None:
            dataInicial = self.dataInicial
        if dataFinal is None:
            dataFinal = self.dataFinal

        # Lendo a variavel de ambiente com chave de acesso à API
        key = os.getenv('exchangerate_key')

        # Definindo parâmetros da API
        url = 'https://api.exchangerate.host/timeframe'
        params = {'access_key': key,  'start_date': dataInicial, 'end_date': dataFinal, 'source': 'USD', 'currencies': 'BRL'}
        
        # Realiza a checagem se a resposta da API é 200 com sucesso se não faz 5 tentativas a cada 10 segundos
        # Também realiza o salvamento de mensagens de log em caso de erro
        for i in range(1, self.tentativas + 1):
            res = rq.get(url, params=params, timeout=30)
            if res.status_code == 200:
                break
            else:
                aux.log('Erro', 'API de cotação do dolar do exchangerate.host ' + str(i) + ' falhou com erro: ' + str(res.status_code))
                time.sleep(self.tempo)
                if i == 5:
                    sys.exit(1) # fecha o código imediatamente

        # Faz a requisição da API e transforma em dicionário
        
        # Gera um log se ocorrer erro na transformação dos dados
        try:

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
            aux.saveParquet(df, pasta)

            # Gera log em caso de sucesso
            aux.log('Sucesso', 'Concluída a atualização dos dados de cotação do dolar de ' + dataInicial + ' até ' + dataFinal)

            # Retorna o dataframe com resultado dessa função
            return df
        # Se ocorrer erro nos tratamentos dos dados gera log de erro e finalza programa
        except Exception as erro:
                    aux.log('Erro', 'API de cotação do dolar do exchangerate.host falhou no tratamento de dados com erro: ' + str(erro))
                    exit(0) # Pra encessar a execução do console
    
    def dadosCurados(self, dataInicial=None, dataFinal=None):
        # Gera um log se ocorrer erro na transformação dos dados
        try:
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
            aux.saveParquet(df_final, pasta)

            # Gera log em caso de sucesso
            aux.log('Sucesso', 'Concluída a atualização das bases bruto e curado')

            return df_final
        
        # Se ocorrer erro nos tratamentos dos dados gera log de erro e finalza programa
        except Exception as erro:
                aux.log('Erro', 'Tratamento dos dados curados falhou com erro ' + str(erro))
                exit(0) # Pra encessar a execução do console
            

