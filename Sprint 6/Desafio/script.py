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
aws_access_key_id = 'ASIAZI2LF2WKTCVIM5WL'
aws_secret_access_key = '01LJy7MP4++kRY3I6/yOJHRDqKqxBG1ByKWwzzhQ'
aws_session_token = 'IQoJb3JpZ2luX2VjELL//////////wEaCXVzLWVhc3QtMSJHMEUCIQDDy1zSQWH/97zx9mOldurNXFheKhHMU93dTzgy5FKIFAIgdJEQXCftPE9TZR/KLDdnyyaGg6gYciB+ZH4zIH/tZmYqnwMIexAAGgw2Mzc0MjM0NDEzMDEiDBoy9S6PkNdknURBpyr8AsesXXBVt/U2gYF6ya2AqU1EStj2T3kuE9WPvg18cOTFAKo9vF3v0lLUbQ8dWhAynSxjRGPf8zZof3ys4I78oWEjjEhgGjaqjF4nOVlkm+t4V9M5/22+QA+0mfd+eLfO8AKsm67G/t4puAHkF0n3Rij4JuUTi5zEJZrTkuc/gVbklQoF5puLvMaK07GR1blHczvhi1lYqGDYsWKdCQKASJPyTGXpxh4YK5QnzOt01Do5hOrtJAz3vcLpqnhPXH1yBSItbOy8ZX9kDH1D7YwYra9QSZrpEGd3tzvT6wpaR2/Xj4soFslKObOXBNpYk1yyUSFxRNb8OxCOPUys9qdRn64PkVOynDP9p5yK/rbvMK10ABMTu0H77GPxVJEiUA7CN/7i7/5N12WbvyU93u9Ip4xUWsUax4esUFw2MU+g9EpGraM1NmEAKUq020cvdCmnku/De6CF3GR7WefiIZMf25INhho88RZjUV7grJp3f2D0bMPhZpXIn2QJDsm0MNTMxbQGOqYB+jOWmzztINTtyYxPn7t3a83wSG6axVi3ovuX8X7R9TeHSD5rlygwdQxk/Ov3vTbTlGC3E6dWRlWdbkeeQ9wCKXQZK7qH/ipJ1Ylo80OOvjfFqh5j73En5o/mFtoZF5jMoV1BKS/HChpHXq/IzQu8cnzYWrCouGuvEEmpW4jOB5orJSfEHQoBb3II+Ieo4xUV+95m8BOrKPFSRcCvmZnx2mJSaqDZJw=='

# Configurar cliente S3 com credenciais temporárias
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

# Nome do bucket
bucket_name = 'desafio-6'
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
