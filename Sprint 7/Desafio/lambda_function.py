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

aws_access_key_id = 'ASIAZI2LF2WK2DIJX7A2'
aws_secret_access_key = 'gigcizgchSSqeaU4toK2ya+44ISSVP4EvexfE0qq'
aws_session_token = 'IQoJb3JpZ2luX2VjEKv//////////wEaCXVzLWVhc3QtMSJGMEQCIF32B6CfeIUQ8uhSX7CTmhJ9+qDirJIZ7cVSsK+tawC/AiA2jFeqz/otmcGHZo4QeHwu0ikisfL48xxGfa/h/RJfXyqoAwiz//////////8BEAAaDDYzNzQyMzQ0MTMwMSIMqWTPqHVqvzruYfQkKvwCeGnySlqZ+n9IMa0YwnZjhJlz1bjcRgxK7dgJxRnddVXchQKL49sN+yipa7cvRPsHLKiTJ4UHOiEvFzfqbzCkpdr20+Tl6V0ivNI6E/eA6iw9YWUfQLV9JUEqOHLTdoYIRHcPuzObBhyHzDn0s7ieDL1Q2tuA65WH0WiJc/gilR/osAfZYZxHyFzouR8vzUAhbEUHiY3sGVl6aRkKPdQDU971Tvn4FDa1eEHDcyhcoG2W4zKJitQ6btaAYvhkk3feuEFYbXIw3Fx9CdrEGI5wHqXlVchf8WGNkyx/RnoMdBAYqTmOgKfxjAg7ZKlpaaoNWce5t3jko17oPQEHGgYcWAuckjJoaw/2l674VTALy12/xNB9TI1j6f9ceQQFSwE1nky++zeueJku5CSqD/CV02VBFh6pjFBWc3vedNiB8NvkV762BsUn+SXVha7pZ7K0XGqZWOkqg797NIiszuzwWIxxKdGcyMqliIly1PUzNyn0gQAEm9eB4LvW9KcwjoWltgY6pwGdYL9OqTpfp72+Agm0FhS+yfuGANDtORuAfKWudcyadIjz9fOMjhxP0jisVssM+U8dUR8qbFsILvy5/5HeKUQT3aMgXNWKAYJnrPge8Z1tRGi5ZlRxKWtf1P0su05CRGjQkcK4nFUJCsmM899nQfOzK9/jgfbcpHJBomUwIHsEPu49s9MuHg216ck/rNB3L05UapfvIqLXj1ExS1sV1AIqB5a5hklv8g=='

# Inicialize o boto3 para S3
s3 = boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token)
bucket_name = 'projeto-pb'

# Obter a data atual
today = datetime.today().strftime('%Y-%m-%d')

def fetch_fantasy_movies():
    movie = Movie()
    genre_id = 14  # ID do gênero fantasia
    page = 1
    all_fantasy_movies = []

    while page <= 50:  # Limita a 5 páginas para simplificação
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

    # Pegar os 200 primeiros
    longest_worst_rated_fantasy_movies = all_fantasy_movies[:200]
    
    # Simplificar dados para CSV
    simplified_movies = [
        {
            'id': movie.get('id'),
            'title': movie.get('title'),
            'release_date': movie.get('release_date'),
            'popularity': movie.get('popularity'),
            'vote_average': movie.get('vote_average'),
            'runtime': movie.get('runtime'),
            'idioma': movie.get('original_language')
        }
        for movie in longest_worst_rated_fantasy_movies
    ]
    
    return simplified_movies

def fetch_top_sci_fi_series():
    tv = TV()
    genre_id = 10765  # ID do gênero sci-fi
    page = 1
    all_sci_fi_series = []

    while page <= 50:  # Limita a 5 páginas para simplificação
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

    # Pegar as 200 séries mais bem avaliadas
    top_sci_fi_series = all_sci_fi_series[:200]
    
    # Simplificar dados para CSV
    simplified_series = [
        {
            'id': series.get('id'),
            'name': series.get('name'),
            'first_air_date': series.get('first_air_date'),
            'popularity': series.get('popularity'),
            'vote_average': series.get('vote_average'),
            'idioma': series.get('original_language')
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
        save_to_csv(fantasy_movies_csv, fantasy_movies, ['id', 'title', 'release_date', 'popularity', 'vote_average', 'runtime', 'idioma'])
        read_csv_and_save_as_json(fantasy_movies_csv, fantasy_movies_json)
        upload_to_s3(fantasy_movies_json, f'Raw/TMDB/JSON/{today}/fantasy_movies.json')

        # Buscar as 10 séries de sci-fi mais bem avaliadas
        sci_fi_series = fetch_top_sci_fi_series()
        save_to_csv(sci_fi_series_csv, sci_fi_series, ['id', 'name', 'first_air_date', 'popularity', 'vote_average', 'idioma'])
        read_csv_and_save_as_json(sci_fi_series_csv, sci_fi_series_json)
        upload_to_s3(sci_fi_series_json, f'Raw/TMDB/JSON/{today}/sci_fi_series.json')

if __name__ == "__main__":
    main()
