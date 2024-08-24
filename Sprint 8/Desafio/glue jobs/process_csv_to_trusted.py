import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Captura dos argumentos de entrada
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MOVIES_INPUT_PATH', 'SERIES_INPUT_PATH', 'OUTPUT_PATH'])

# Inicialização do contexto Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos de entrada e saída
movies_input_path = args['MOVIES_INPUT_PATH']
series_input_path = args['SERIES_INPUT_PATH']
output_path = args['OUTPUT_PATH']

# Função para processar CSV e salvar como Parquet
def process_csv_to_parquet(input_path, output_subfolder, columns_to_include):
    try:
        # Leitura do arquivo CSV como um DynamicFrame
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [input_path]},
            format="csv",
            format_options={"withHeader": True}  # Considera que o CSV tem cabeçalho
        )

        # Converte o DynamicFrame para DataFrame para transformação
        df = dynamic_frame.toDF()

        # Filtra as colunas que você deseja manter
        df_filtered = df.select(columns_to_include)

        # Converte de volta para DynamicFrame
        dynamic_frame_filtered = DynamicFrame.fromDF(df_filtered, glueContext, "dynamic_frame_filtered")

        # Gravação em Parquet na Trusted Zone
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame_filtered,
            connection_type="s3",
            connection_options={"path": f"{output_path}/{output_subfolder}/"},
            format="parquet"
        )
        
        print(f"Arquivo {input_path} processado e salvo em {output_path}/{output_subfolder}/")
        
    except Exception as e:
        print(f"Erro ao processar {input_path}: {str(e)}")

# Lista de colunas a serem incluídas na conversão para Parquet
columns_to_include_movies = ["id", "tituloOriginal", "anoLancamento","tempoMinutos", "genero", "notaMedia", "numeroVotos", "generoArtista", "personagem", "nomeArtista"]
columns_to_include_series = ["id", "tituloOriginal", "anoLancamento","tempoMinutos", "genero", "notaMedia", "numeroVotos", "generoArtista", "personagem", "nomeArtista"]

# Processa o arquivo movies.csv
process_csv_to_parquet(movies_input_path, "csv/movies", columns_to_include_movies)

# Processa o arquivo series.csv
process_csv_to_parquet(series_input_path, "csv/series", columns_to_include_series)

# Finaliza o job
job.commit()
