#==================================================
# TÍTULO: func_aux.py
# DESCRIÇÃO: Nesse arquivo serão desenvolvidas todas as funções auxiliares necessárias para o programa.
#==================================================

import os
import pandas as pd
from datetime import datetime as dt

class funcAux():
    def __init__(self):
        # Caminho da pasta de log, se não existe cria uma e variaveis de número de tentativas e tempo
        self.log_arquivo = os.getenv('output') + 'logs/logs_pipeline.csv'
        os.makedirs(os.path.dirname(self.log_arquivo), exist_ok=True)
    
    # Essa função faz o salvamento dos arquivos em .parquet
    def saveParquet(self, df, pasta):
        # Loop por ano e mês, no final vai salvar na estrutura de pastas
        for (year, month), group in df.groupby(['year', 'month']):
            # Pasta do ano com formato year-YYYY
            year_path = os.path.join(pasta, f'year-{year}')
            os.makedirs(year_path, exist_ok=True)
            
            # Caminho do arquivo: month-MM.parquet
            file_path = os.path.join(year_path, f'month-{month:02d}.parquet')
            
            # Salvar .parquet removendo coluna index do dataframe
            group.drop(['year', 'month'], axis=1).to_parquet(file_path, engine='pyarrow', index=False)
    
    # Essa função salva os logs de execuções do programa
    def log(self, status, mensagem):
        df_erro = pd.DataFrame({
                        'status': [status],
                        'mensagem': mensagem,
                        'data_hora': [dt.now().strftime('%Y-%m-%d %H:%M:%S')]
                    })
        
        # Exibe mensagem na tela do console se estiver aberta
        print('Status:', status + ' / Mensagem: ' + mensagem)

        # Adiciona uma linha no arquivo, caso já exista cria um novo arquivo
        df_erro.to_csv(self.log_arquivo, index=False, mode='a', header=not os.path.exists(self.log_arquivo))