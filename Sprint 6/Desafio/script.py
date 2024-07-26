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
aws_access_key_id = 'ASIAZI2LF2WKWWMPAWKW'
aws_secret_access_key = 'X38HCKWDBZTuLMTdNHxatCDzEBkWScRXJVs3yJfW'
aws_session_token = 'IQoJb3JpZ2luX2VjEEIaCXVzLWVhc3QtMSJIMEYCIQCJwpNqugpg0ueuKbCwa9vKGZyR/OKCL1vTSsUtTz9ARQIhAIU8KMht0YSu3mDVtXcer49UQSMzIqIYTl8/w/2yHNheKp8DCBoQABoMNjM3NDIzNDQxMzAxIgzb8SL4EdgywZnzD3Uq/ALOy7NeOanTFaINNGYz3fCThgAnPIB8RlJiONGhM3mMz9sipr1Gdp4RDcu02Me4+dKwJosk6zYPSYlDvTV6JLhCTC3UGn7OZyY5IRAQLvgr1gPJQ/2leUN8f8CeIRAHf2P0exGvR3jB7CjVRXUM1Z5jF1Wui8gMHc9ouexXXAGKjDe1Mv5cOOSkwIuQF+TTkNkJsUiiBLneZ4ePlhkqB/2bcmIKCGTcnAogVZjc1M0crMjODNtJUYoEU2ro2pW4k82SX6IIhvvgmzOCIg0M0I3j6u20NNUWsmqvdi72OmxemudSW1P2vcrEhmbN61olsi5bDf7TLlhInnmvE0T4u/bHE5/ULhOLNw3GNOVc4WiHZvbZUZmwSt2J8oHzlJjCtSEBF5eIUFsCLSN2cQ+kaujMxOsPK8V8JSLJEtFKgwAI+F4KTKd4iTXNNVZflhJRvIGyzdX/XjzGbl7csUHp3hgfKwKLTg1D5BfsHVx6mgQrWlmI1C6vSn1503fxjDCSluW0BjqlAcwa0tFf9rlDQiRmGka3xL8IXJkACvB6JauOl59T5Yx56jI1jjtV1mv+y40Dnl0ebKo/ieSe2ocCF4vVCrXYC8fDwVnnVGp6Kx+TlaiXmc4uU+1p44IJNP1BTp7CPvjInrkc80tSog2/5Lh9DjUEt7U1LRSqB45MnyrqmH3Z+lzA6VkBiwcnakYlAiiwumoGc2+TQRQL2nUTpfsLpk+yAnPz1LNgRQ=='

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

