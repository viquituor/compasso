import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

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

# Função para processar JSON e salvar como Parquet
def process_json_to_parquet(input_path, output_subfolder):
    # Leitura do arquivo JSON como um DynamicFrame
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [input_path]},
        format="json"
    )

    # Converte o DynamicFrame para DataFrame para transformação
    df = dynamic_frame.toDF()

    # Renomeia as colunas conforme necessário
    if 'title' in df.columns:
        df = df.withColumnRenamed("id", "id")
        df = df.withColumnRenamed("title", "tituloPincipal")
        df = df.withColumnRenamed("release_date", "anoLancamento")
        df = df.withColumnRenamed("popularity", "popularidade")
        df = df.withColumnRenamed("vote_average", "notaMedia")
        df = df.withColumnRenamed("runtime", "tempoMinutos")
    elif 'name' in df.columns:
        df = df.withColumnRenamed("id", "id")
        df = df.withColumnRenamed("name", "tituloPincipal")
        df = df.withColumnRenamed("first_air_date", "anoLancamento")
        df = df.withColumnRenamed("popularity", "popularidade")
        df = df.withColumnRenamed("vote_average", "notaMedia")

    # Converte de volta para DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    # Gravação em Parquet na Trusted Zone
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"{output_path}/{output_subfolder}/"},
        format="parquet"
    )

# Processa o arquivo movies.json
process_json_to_parquet(movies_input_path, "movies")

# Processa o arquivo series.json
process_json_to_parquet(series_input_path, "series")

# Finaliza o job
job.commit()
