import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configurando credenciais temporárias diretamente no código
aws_access_key_id = 'ASIAZI2LF2WKX3ZLXO6O'

aws_secret_access_key = '2YZBxp6JBtIQgC+gtHFOMPU+CDDASbPlFoxGnXoq'

aws_session_token = 'IQoJb3JpZ2luX2VjEKf//////////wEaCXVzLWVhc3QtMSJHMEUCIGLU6Bg/7ypdO29KuS0QgXxyZViGegOzNzAkNQV2+F2fAiEAgp+hijAwB2b/VU8eIByG5T8vRmRjL4M9DK6IvwUePvkqnwMIYBAAGgw2Mzc0MjM0NDEzMDEiDHdNvKk9hY+S4RKohSr8AgNlf1zZztKCVzHjLu+m6qWTb9DHSEasrnPDDJ1+z7WvuQzoITQnU3aYUIl8BmeTYIDL1WPwM+3h4BtK/aXnL+AgexpHcBX4VtX4aQIs12mDlt0I4TVDgqZ0eJgJHVad/0oNiLgojthMqyP4DRIQNLzm52sNAGA1ezXl6l5ponARBtPuZ0ZXUgSfPFPa7u9q0bkgD+yvyX95mNIOPc6zNYXGbje0xL6CiBQ71vbZBlcv2aLrd7xJs1pkrkSLNhuLOyxDxw7EsrIlcf9vtOgxcHGuW4MEvSnrgqHfVMBqANCkFKT2riN+OqyzrlkuWPuHXthTFa/PelTSxeWN0QWEF8gI2VW1sya8Bu/Uk1B5xqkArTYLhjLbc3IAbYNJ0OshzMXHNqAze/exNkJ8OILiqpaVCE19J60ivTP9E+0bD6yUTdAdU/0fNHKgSfO9hNiwyQJh2kEguhjInvFAsO7IO+UgYRIlo5UGQpLo/T6Cq+VxoGZ1W0/eKmxHNX9XMNWBi7QGOqYBjak1Cv0ougf0l7Qkt9Iz7IPHXTrwVDW1fITr6f6h0HHJCEi6OqeuOP5AWwyOfTcnjuURn1Kap7XxsSyuQht2pBac1V5ZALS4h83TCbrkG+qGSGFTabfFPHbw3zpwwsccgTVjFLedp8Ni/RfH0MybFGzxrXIYFKWFGif/XykkXwxC52EMEP9MfRqPm/j/XEcDxpdYKvxsk8/lQ1Xwwc1MBDGgp5AyxQ=='
# Nome do bucket e do arquivo CSV
bucket_name = 'desafio5'
file_key = 'bilheteria-diaria-obras-por-exibidoras-2024-05-s05.csv'

# Consulta SQL para S3 Select
sql_query = """
SELECT COUNT(*) FROM S3Object
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
