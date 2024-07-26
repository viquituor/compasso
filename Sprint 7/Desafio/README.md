# Desafio: Ingestão de Dados do TMDB para o S3 com AWS Lambda

## Objetivo

O objetivo é praticar a combinação de conhecimentos vistos no Programa, fazendo um mix de tudo que já foi dito.

## 1. Entrega 2: Ingestão de API

Nesta etapa do desafio iremos capturar dados do TMDB via AWS Lambda realizando chamadas de API. Os dados coletados devem ser persistidos em Amazon S3, camada RAW Zone.

### 1.1 Abaixo segue as importações e suas ações dentro do script

```python
import csv
import json
import os
import tempfile
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError
from tmdbv3api import TMDb, Movie, TV

```

### tempfile

foi utilizado para criar arquivos temporarios como o csv para ser transcrito para json

### boto3

foi utilizado para a incerção de dados na aws s3

### datatime

foi utilizado para criar as pastas dos dias que os dados são inseridos no bukect

### botocore.exceptions

foi utilizado para indentificar certos erros da certificação da aws

### tmdbv3api

foi utilizado para consutar os dados de filmes e series

## 1.2 Salvamento em csv e tranformação para json

Para concluir o desafio foi necessario que os dados da consulta da API fossem salvos em um arquivo temporario csv e depois transformdo para um aquivo json, pois foi a unica forma que consegui de salvar os dados em json

```python
import csv
import json
import os
import tempfile
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError
from tmdbv3api import TMDb, Movie, TV

# Inicialize o TMDb
tmdb = TMDb()
tmdb.api_key = 'ce0842a8df0d5d21acf0a8ce237ca4d7'

# Inicialize o boto3 para S3
s3 = boto3.client('s3')
bucket_name = 'projeto-pb'

# Obter a data atual
today = datetime.today().strftime('%Y-%m-%d')

def fetch_fantasy_movies():
    movie = Movie()
    # ID do gênero fantasia
    genre_id = 14  
    page = 1
    all_fantasy_movies = []

 # Limita a 5 páginas para simplificação
    while page <= 5: 
        try:
            response = movie.popular(page=page)
            filtered_movies = [m for m in response.get('results', []) if genre_id in m.get('genre_ids', [])]
            all_fantasy_movies.extend(filtered_movies)
            page += 1
        except Exception as e:
            print(f'Erro ao buscar filmes de fantasia: {str(e)}')
            break

    # Ordenar por duração (mais longos) e avaliação (piores)
    all_fantasy_movies.sort(key=lambda x: (x.get('runtime', 0), -x.get('vote_average', 0)))

    # Pegar os 30 primeiros
    longest_worst_rated_fantasy_movies = all_fantasy_movies[:30]
    
    # Simplificar dados para CSV
    simplified_movies = [
        {
            'id': movie.get('id'),
            'title': movie.get('title'),
            'release_date': movie.get('release_date'),
            'popularity': movie.get('popularity'),
            'vote_average': movie.get('vote_average'),
            'runtime': movie.get('runtime')
        }
        for movie in longest_worst_rated_fantasy_movies
    ]
    
    return simplified_movies

def fetch_top_sci_fi_series():
    tv = TV()
    genre_id = 10765  # ID do gênero sci-fi
    page = 1
    all_sci_fi_series = []

    while page <= 5:  # Limita a 5 páginas para simplificação
        try:
            response = tv.popular(page=page)
            filtered_series = [s for s in response.get('results', []) if genre_id in s.get('genre_ids', [])]
            all_sci_fi_series.extend(filtered_series)
            page += 1
        except Exception as e:
            print(f'Erro ao buscar séries de sci-fi: {str(e)}')
            break

    # Ordenar por avaliação (mais bem avaliadas)
    all_sci_fi_series.sort(key=lambda x: -x.get('vote_average', 0))

    # Pegar as 30 séries mais bem avaliadas
    top_sci_fi_series = all_sci_fi_series[:30]
    
    # Simplificar dados para CSV
    simplified_series = [
        {
            'id': series.get('id'),
            'name': series.get('name'),
            'first_air_date': series.get('first_air_date'),
            'popularity': series.get('popularity'),
            'vote_average': series.get('vote_average')
        }
        for series in top_sci_fi_series
    ]
    
    return simplified_series

def save_to_csv(file_name, data, headers):
    # Salvar os dados em um arquivo CSV
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

def read_csv_and_save_as_json(csv_file_name, json_file_name):
    # Ler o arquivo CSV e salvar como JSON
    with open(csv_file_name, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        data = [row for row in reader]

    with open(json_file_name, mode='w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)

def upload_to_s3(file_name, s3_path):
    # Fazer upload do arquivo JSON para o bucket S3
    try:
        s3.upload_file(file_name, bucket_name, s3_path)
        print(f"Arquivo '{file_name}' enviado para '{s3_path}' no bucket '{bucket_name}' com sucesso.")
    except FileNotFoundError:
        print(f"Arquivo '{file_name}' não encontrado.")
    except NoCredentialsError:
        print("Credenciais AWS não encontradas.")

def main():
    # Buscar os filmes de fantasia mais longos e piores avaliados
    with tempfile.TemporaryDirectory() as temp_dir:
        fantasy_movies_csv = os.path.join(temp_dir, 'fantasy_movies.csv')
        fantasy_movies_json = os.path.join(temp_dir, 'fantasy_movies.json')
        sci_fi_series_csv = os.path.join(temp_dir, 'sci_fi_series.csv')
        sci_fi_series_json = os.path.join(temp_dir, 'sci_fi_series.json')
        
        # Busca, salvamento, conversão e upload dos dados
        fantasy_movies = fetch_fantasy_movies()
        save_to_csv(fantasy_movies_csv, fantasy_movies, ['id', 'title', 'release_date', 'popularity', 'vote_average', 'runtime'])
        read_csv_and_save_as_json(fantasy_movies_csv, fantasy_movies_json)
        upload_to_s3(fantasy_movies_json, f'Raw/TMDB/JSON/{today}/fantasy_movies.json')

        # Buscar as 10 séries de sci-fi mais bem avaliadas
        sci_fi_series = fetch_top_sci_fi_series()
        save_to_csv(sci_fi_series_csv, sci_fi_series, ['id', 'name', 'first_air_date', 'popularity', 'vote_average'])
        read_csv_and_save_as_json(sci_fi_series_csv, sci_fi_series_json)
        upload_to_s3(sci_fi_series_json, f'Raw/TMDB/JSON/{today}/sci_fi_series.json')

if __name__ == "__main__":
    main()

```

### 2. Incerção no AWS S3 bucket pelo Lambda

2.1 Upload do código Python para uma função Lambda no console AWS.

![ADICIONADO SCRIPT NO LAMBDA](/Sprint%207/Desafio/Evidencias/lambda.jpeg)

2.2 Configurado uma trigger (gatilho) no Lambda para executar periodicamente usando o CloudWatch Events ou Amazon EventBridge. Criado uma nova regra para agendar a execução.

![CRIAÇÃO DE CRONOGRAMA DE EXECUÇÃO DO LAMBDA](/Sprint%207/Desafio/Evidencias/evet.jpeg)

2.3 Evidências da Criação dos Arquivos
Verifique os arquivos JSON criados e enviados para o bucket S3. Assegure-se de que os arquivos estão no caminho especificado e no formato correto.

![EVIDENCIAS DA CRIAÇÃO DOS ARQUIVOS](/Sprint%207/Desafio/Evidencias/bucket.jpeg)
