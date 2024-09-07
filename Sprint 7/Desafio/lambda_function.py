import json
import boto3
from tmdbv3api import TMDb, TV, Movie
import os

# Inicialize o TMDb
tmdb = TMDb()
tmdb.api_key = os.getenv('TMDB_API_KEY')  # Obter a chave da API do TMDb de variáveis de ambiente

# Inicialize o cliente S3
s3_client = boto3.client('s3')

def fetch_series(genres):
    tv = TV()
    page = 1
    all_series = []

    while page <= 500:
        try:
            response = tv.popular(page=page)
            filtered_series = [s for s in response if any(genre in s.genre_ids for genre in genres)]
            all_series.extend(filtered_series)
            page += 1
        except Exception as e:
            print(f'Erro ao buscar séries: {str(e)}')
            break
    
    return all_series

def fetch_movies(genres):
    movie = Movie()
    page = 1
    all_movies = []

    while page <= 500:
        try:
            response = movie.popular(page=page)
            filtered_movies = [m for m in response if any(genre in m.genre_ids for genre in genres)]
            all_movies.extend(filtered_movies)
            page += 1
        except Exception as e:
            print(f'Erro ao buscar filmes: {str(e)}')
            break
    
    return all_movies

def convert_to_dict(item_list, item_type):
    item_dict_list = []
    for item in item_list:
        item_dict = {
            "id": item.id,
            "origin_country": list(getattr(item, 'origin_country', [])) if item_type == 'series' else None,
            "original_language": str(item.original_language),
            "name": str(getattr(item, 'name', None) if item_type == 'series' else getattr(item, 'title', None)),
            "overview": str(item.overview),
            "popularity": float(item.popularity),
            "release_date": str(getattr(item, 'first_air_date', None) if item_type == 'series' else getattr(item, 'release_date', None)),
            "vote_average": float(item.vote_average),
            "vote_count": int(item.vote_count)
        }
        item_dict_list.append(item_dict)
    return item_dict_list

def upload_to_s3(bucket_name, file_name, data):
    # Upload os dados para o bucket S3 como JSON
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data, ensure_ascii=False, indent=4),
            ContentType='application/json'
        )
        print(f"Arquivo '{file_name}' enviado com sucesso para o bucket S3 '{bucket_name}'")
    except Exception as e:
        print(f"Erro ao fazer upload para o S3: {str(e)}")

def lambda_handler(event, context):
    # Gêneros de fantasia e ficção científica
    fantasy_genre_id = 14   # ID do gênero fantasia para filmes
    sci_fi_genre_id = 878   # ID do gênero ficção científica para filmes
    series_genre_id = 10765 # ID do gênero ficção científica e fantasia para séries

    # Nome do bucket S3 (obtido de variáveis de ambiente)
    s3_bucket = os.getenv('S3_BUCKET_NAME')

    # Buscar todas as séries de fantasia e ficção científica
    genres = [series_genre_id]
    all_series = fetch_series(genres)

    # Converter a lista de séries para uma lista de dicionários
    series_dict = convert_to_dict(all_series, 'series')

    # Upload das séries para o S3
    upload_to_s3(s3_bucket, 'fantasy_and_sci_fi_series.json', series_dict)

    # Buscar todos os filmes de fantasia e ficção científica
    genres = [fantasy_genre_id, sci_fi_genre_id]
    all_movies = fetch_movies(genres)

    # Converter a lista de filmes para uma lista de dicionários
    movies_dict = convert_to_dict(all_movies, 'movies')

    # Upload dos filmes para o S3
    upload_to_s3(s3_bucket, 'fantasy_and_sci_fi_movies.json', movies_dict)

    return {
        'statusCode': 200,
        'body': json.dumps('Dados enviados para o bucket S3 com sucesso!')
    }
