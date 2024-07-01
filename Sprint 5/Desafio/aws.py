import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configurando credenciais temporárias diretamente no código
aws_access_key_id = 'ASIAZI2LF2WKYW7JGKJS'

aws_secret_access_key = 'aQqUKDqC/P08glR+uRyWd7pu+lvsndR42RJFAaVt'

aws_session_token = 'IQoJb3JpZ2luX2VjEJf//////////wEaCXVzLWVhc3QtMSJHMEUCIHSlsuX35/qgvhIOKwAMxiI+8ccpMv+xKEmcgRr9T7f/AiEA8reFriY9TYBH55TE6IdflgUKVGdDdIzuqrKeQFjyZqoqnwMIUBAAGgw2Mzc0MjM0NDEzMDEiDPMacU8s4aQPnQwUfir8Ag/DHJ+BhiiZuyL9ccRRGi2WHZVt1gxfwpHigh7Zsr9geFfRQr8Jg1cgrLfoA7J+CaL/0HTTS8Xz4hn59Az41FdpCyBUHABY5jYBjDJEUdRI8xBkFRBHg/SP2g02eTScFTwvsYwfGTrcjoyZisJlCJVVYOMqyewDCu1XeK1Cs61fzO5F+WhtVPXnJcie3y7GVGS0zdXwSmPd6weMvUeWKSSOabIDy/uT4aZenqDIqA5remz6KnupFmW1twriDu707Dt1Lh7eSZSD7nIW8MChhx+jj/PeVLRH6CAADTzQkOUiEDFq/u1Fi1PCK8Q87k2o1sWCa9+rbtH5IYbj+gEKJImI5LCyot4mmTK/C7MvBO31p73TtMPoEFNUoSEGHLZHPII9RDUP7iFpS3QcDC6QL4UnAspq8N46X4ityhu5Va4ycbh0NCex3rd8elsnkuwdYmQx5YVSESqGgx2MXRIWrXd0iByKCSYo/zBAD0vpKpw9KLU1Cbki41KFyJRvMIq2h7QGOqYB/NQWyAxkiADHTqWAY1m1Ai/tyFPgR4RufyU8+HrJoId8nCzn4t/C2IOT98UjxfTF4nhjUTZwvNJhb81+XMpe6nW56pescgD7P2YTSt/n9nb9RsT1Zx3O4oH6GmH4iWwagiSeNRiM1muEvNbEArK0+ZkbUExhJNP7twvyqdfcIMg6GKRISTfmnHqR6NvukL92pbS1iMpSXYuW2iOJA+3/1VT3jozs4Q=='
# Nome do bucket e do arquivo CSV
bucket_name = 'desafio5'
file_key = 'bilheteria-diaria-obras-por-exibidoras-2024-05-s05.csv'

# Consulta SQL para S3 Select
sql_query = """
   SELECT  
    DATA_EXIBICAO, 
    LOWER(PAIS_OBRA) AS lower_PAIS_OBRA,   
    CASE WHEN PAIS_OBRA = 'BRASIL' THEN 'NACIONAL' ELSE 'INTERNACIONAL' END AS OBRAS_NACIONAIS, 
    TITULO_ORIGINAL, 
    TITULO_BRASIL, 
    PAIS_OBRA, 
    PUBLICO
FROM 
    S3Object
WHERE 
    PUBLICO < '99' AND PUBLICO > '20'

"""

try:
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )

    s3 = session.client('s3')

    response = s3.select_object_content(
        Bucket=bucket_name,
        Key=file_key,
        ExpressionType='SQL',
        Expression=sql_query,
        InputSerialization={'CSV': {"FileHeaderInfo": "USE", "FieldDelimiter": ";"}},
        OutputSerialization={'CSV': {}},
    )

    for event in response['Payload']:
        if 'Records' in event:
            print(event['Records']['Payload'].decode('utf-8'))

except NoCredentialsError:
    print("Erro: Credenciais AWS não encontradas.")
except PartialCredentialsError:
    print("Erro: Credenciais AWS incompletas.")
except Exception as e:
    print(f"Erro: {e}")
