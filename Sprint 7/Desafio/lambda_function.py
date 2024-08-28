import json
import os
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
from tmdbv3api import TMDb, Movie, TV

# Inicialize o TMDb
tmdb = TMDb()
tmdb.api_key = 'ce0842a8df0d5d21acf0a8ce237ca4d7'

# Inicialize o boto3 para S3
s3 = boto3.client('s3',
    aws_access_key_id='ASIAZI2LF2WKX2Q7GZ33',
    aws_secret_access_key='Hzg4Pw8GKwcMaxwn1aP1OyBLr8cmOStjprL0ull9',
    aws_session_token='IQoJb3JpZ2luX2VjEB4aCXVzLWVhc3QtMSJIMEYCIQCpSbFtJdxAI79haYMtu8IZ6Vc6289vLkiqL0C4w1uYcQIhAOCyhyB789vvL1ckIYqfUVTd5XNuW6l1b1Bb7TX5/NA/Kp8DCDcQABoMNjM3NDIzNDQxMzAxIgx/dMDsO5cv30mdLEMq/AK9gpsCy4BhQlcppNTEgvK2sQDzCvT8pc47ZJfz6QYtHzvBEl5Mh/hqYxxuS1TXka6ISXLN/Y4ey5V9MCHPSO/0II0cISam8ogy+HBJJ+m2+hpiPpfp85DQ1zUBuzZXPyZRUol84xYDiUrVIGmU5ZXLE7xHJf+SrgdISm6apou65oI8CfcayLIuDtCVk7K3sDmolpFhu2sTwyN1fePTNYRqaOxzJI0nYhnNSwTLBjEleEsd6fkmJptdJ67uXRIXEgdOfJimPgihhaiXgY6r56NMehtMnsYPq0061RnTyiBbBBssph8FGS0emp4i03SR74JIuEdrNfaCAGvCII88azsXxLhQ157/DSSBzRcdRb+T7SrNLlJ281/y8SKWHzZTcWp78RqfWerPSWnH8oTEa+XxXU08uP1ggDV8mK6qwwNH3Q6R6Kypdofs7AhUn/SMgnqpmYf0Wo4K9nJPyoER/kM8JC8KTtSJty7EXd1Us1Dq+xT3UfkoGHQwAxBUDjC1r762BjqlAaklq3e4sCVrsnc+iSf2PSQnIDXvcBwk4PEP4qHpojzhiXRISkAl6KFj3VD4j5F7DmAqlYuGzZ/tbvZUhLGc1XfEcjo28M+tzRamdeREeu23u89mSHInj/4i/AoygkKRenmoTWMYSYJ4eFu1Z69M+4u7WRCX8zdUr4BKwrZ/SiBLZleJY7nW2F5En+O/8BaszOfRiKr62OtOd2YktcWh0e5rN0NqIw==')  # Se necessário

bucket_name = 'projeto-pb'

# Obter a data atual
today = datetime.today().strftime('%Y-%m-%d')

def fetch_fantasy_movies():
    movie = Movie()
    genre_id = 14  # ID do gênero fantasia
    page = 1
    all_fantasy_movies = []

    while page <= 500:  # Limita a 500 páginas para simplificação
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
    
    # Simplificar dados para JSON
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

    while page <= 500:  # Limita a 500 páginas para simplificação
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
    
    # Simplificar dados para JSON
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

def save_to_json(file_name, data):
    # Salvar os dados em um arquivo JSON
    with open(file_name, mode='w', encoding='utf-8') as json_file:
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
    # Definir os nomes dos arquivos JSON no diretório atual
    fantasy_movies_json = 'fantasy_movies.json'
    sci_fi_series_json = 'sci_fi_series.json'
    
    # Busca e salvamento dos dados
    fantasy_movies = fetch_fantasy_movies()
    save_to_json(fantasy_movies_json, fantasy_movies)
    print(f"Filmes de fantasia salvos em '{fantasy_movies_json}'")

    sci_fi_series = fetch_top_sci_fi_series()
    save_to_json(sci_fi_series_json, sci_fi_series)
    print(f"Séries de ficção científica salvas em '{sci_fi_series_json}'")

    # Upload dos arquivos para o S3
    upload_to_s3(fantasy_movies_json, f'Raw/TMDB/JSON/{today}/fantasy_movies.json')
    upload_to_s3(sci_fi_series_json, f'Raw/TMDB/JSON/{today}/sci_fi_series.json')

if __name__ == "__main__":
    main()
