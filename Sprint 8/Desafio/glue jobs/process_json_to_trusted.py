import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Captura dos argumentos
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

def process_json_to_parquet(input_path, output_subfolder):
    try:
        # Leitura do arquivo JSON como um DynamicFrame
        logger.info(f"Lendo dados de {input_path}...")
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [input_path]},
            format="json"
        )

        # Converte o DynamicFrame para DataFrame para transformação
        df = dynamic_frame.toDF()

        # Renomeia as colunas conforme necessário
        rename_columns = {
            "title": "tituloPrincipal",
            "release_date": "anoLancamento",
            "name": "tituloPrincipal",
            "first_air_date": "anoLancamento",
            "popularity": "popularidade",
            "vote_average": "notaMedia",
            "idioma": "idioma"
        }
        
        for old_name, new_name in rename_columns.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        # Converte de volta para DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

        # Gravação em Parquet na Trusted Zone
        logger.info(f"Gravando dados em formato Parquet na pasta {output_subfolder}...")
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={"path": f"{output_path}/{output_subfolder}/"},
            format="parquet"
        )
        logger.info(f"Dados processados e gravados com sucesso em {output_subfolder}.")

    except Exception as e:
        logger.error(f"Erro ao processar dados de {input_path}: {e}")

# Processa o arquivo movies.json
process_json_to_parquet(movies_input_path, "json/movies")

# Processa o arquivo series.json
process_json_to_parquet(series_input_path, "json/series")

# Finaliza o job
job.commit()
