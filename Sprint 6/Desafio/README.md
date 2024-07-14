# Desafio de Ingestão Batch de Dados de Filmes e Séries - Documentação

## Consultas em Forma de Pergunta

1. **Quantos filmes do gênero 'Fantasy' estão presentes no arquivo `movies.csv`?**
2. **Quantas séries de TV do gênero 'Sci-Fi' estão presentes no arquivo `series.csv`?**

---

## Etapas do Desafio

### 1. Leitura e Tratamento dos Arquivos CSV

    Para começar, lemos e tratamos os arquivos CSV `movies.csv` e `series.csv` usando a biblioteca pandas. Este passo garante que os dados sejam carregados corretamente para posterior processamento.

    ```python
    import pandas as pd

    # Definindo o caminho dos arquivos CSV
    movies_file_path = '/app/data/movies.csv'
    series_file_path = '/app/data/series.csv'

    # Lendo os arquivos CSV
    movies_df = pd.read_csv(movies_file_path, delimiter='|')
    series_df = pd.read_csv(series_file_path, delimiter='|')

    # Exibindo as primeiras linhas dos DataFrames
    print(movies_df.head())
    print(series_df.head())

### 2. Upload para o S3

Usamos a biblioteca boto3 para interagir com o serviço S3 da AWS. Aqui, configuramos as credenciais e realizamos o upload dos dados tratados para o bucket S3 seguindo a estrutura especificada.

    ```python
    import boto3
    import pandas as pd
    from io import StringIO
    import os

    # Configurando as credenciais temporárias da AWS
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN')

    # Criando a sessão boto3
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )

    # Criando o cliente S3
    s3_client = session.client('s3')

    # Função para upload do DataFrame para o S3
    def upload_to_s3(df, bucket_name, file_key):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

    # Definindo os parâmetros do S3
    bucket_name = 'data-lake-do-fulano'
    movies_file_key = 'Raw/Local/CSV/Movies/2022/05/02/movies.csv'
    series_file_key = 'Raw/Local/CSV/Series/2022/05/02/series.csv'

    # Fazendo upload dos arquivos para o S3
    upload_to_s3(movies_df, bucket_name, movies_file_key)
    upload_to_s3(series_df, bucket_name, series_file_key)

### 3. Criação do Docker Container

Criamos um Docker container. O Dockerfile especifica o ambiente necessário para executar nosso script Python.

    # Usando uma imagem base do Python

    FROM python:3.9-slim

    # Definindo o diretório de trabalho

    WORKDIR /app

    # Copiando os arquivos necessários para o container

    COPY script.py /app/script.py
    COPY requirements.txt /app/requirements.txt

    # Instalando as dependências

    RUN pip install --no-cache-dir -r requirements.txt

    # Comando para executar o script

    CMD ["python", "script.py"]

### 4.Executando o Container com Volume

Montamos um volume para acessar os arquivos CSV do host e executamos o container para processar e carregar os dados no S3.

    docker build -t ingest-data .
    docker run --rm -v "C:\Users\User\OneDrive\Área de Trabalho\trainee-repo-template\Sprint 6\Desafio:/app/data" ingest-data

### Com a execução bem-sucedida, os arquivos CSV foram carregados no bucket S3 conforme a estrutura especificada
