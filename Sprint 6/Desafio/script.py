import boto3
from datetime import datetime
import pandas as pd

# Função para ler o arquivo CSV e tratar os dados
def read_and_clean_csv(file_path, delimiter='|'):
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
        df = df.fillna('')  # Preencher valores ausentes
        return df
    except pd.errors.ParserError as e:
        print("Erro ao ler o arquivo CSV:", e)
        exit()
    except FileNotFoundError as e:
        print("Arquivo não encontrado:", e)
        exit()

# Função para enviar arquivos ao S3
def upload_to_s3(df, bucket, key):
    csv_buffer = df.to_csv(index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer)

# Credenciais temporárias da AWS
aws_access_key_id = 'ASIAZIWWMPAWKW'
aws_secret_access_key = 'X38HCKWDBZTEBkWScRXJVs3yJfW'
aws_session_token = 'IQoJb3JpS5yAnPz1LNgRQ=='

# Configurar cliente S3 com credenciais temporárias
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

# Nome do bucket
bucket_name = 'projeto-pb'
date_path = datetime.now().strftime('%Y/%m/%d')

# Caminhos para os arquivos CSV
movies_file_path = '/app/data/movies.csv'
series_file_path = '/app/data/series.csv'

# Ler e tratar os dados dos arquivos CSV
print("Lendo arquivo movies.csv...")
movies_df = read_and_clean_csv(movies_file_path)
print("Arquivo movies.csv lido com sucesso.")

print("Lendo arquivo series.csv...")
series_df = read_and_clean_csv(series_file_path)
print("Arquivo series.csv lido com sucesso.")

# Definindo a estrutura RAW Zone para os arquivos
movies_file_key = f'Raw/Local/CSV/Movies/{date_path}/movies.csv'
series_file_key = f'Raw/Local/CSV/Series/{date_path}/series.csv'

# Carregar arquivos no S3
print(f"Carregando arquivo movies.csv para {bucket_name}/{movies_file_key}...")
upload_to_s3(movies_df, bucket_name, movies_file_key)
print(f"Arquivo movies.csv carregado com sucesso para {bucket_name}/{movies_file_key}.")

print(f"Carregando arquivo series.csv para {bucket_name}/{series_file_key}...")
upload_to_s3(series_df, bucket_name, series_file_key)
print(f"Arquivo series.csv carregado com sucesso para {bucket_name}/{series_file_key}.")

# COMANDOS USADOS PARA CONSTRUÇÃO DA IMAGEM DOCKER E PARA A EXEUÇÃO:

# docker build -t ingest-data .  

#docker run --rm -v "C:\Users\User\OneDrive\Área de Trabalho\trainee-repo-template\Sprint 6\Desafio:/app/data" ingest-data

