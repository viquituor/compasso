import sys
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
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

# Função para ajustar o esquema e salvar como Parquet
def adjust_schema_and_save(input_path, output_path, schema):
    try:
        # Leitura do arquivo Parquet como um DynamicFrame
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [input_path]},
            format="parquet"
        )

        # Converte o DynamicFrame para DataFrame para transformação
        df = dynamic_frame.toDF()
        
        # Imprimir esquema original e alguns registros para debug
        print(f"Esquema original de {input_path}:")
        df.printSchema()
        df.show(5)

        # Aplica o esquema desejado ao DataFrame
        for col_name, col_type in schema.items():
            df = df.withColumn(col_name, df[col_name].cast(col_type))

        # Verificar o esquema após conversão
        print(f"Esquema ajustado de {input_path}:")
        df.printSchema()
        df.show(5)

        # Converte de volta para DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

        # Gravação em Parquet na camada Refined
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={"path": output_path, "partitionKeys": []},
            format="parquet"
        )

        print(f"Arquivo processado e salvo em {output_path}")

    except Exception as e:
        print(f"Erro ao processar {input_path}: {str(e)}")

# Definição do esquema para movies
movies_schema = {
    "id": StringType(),
    "tituloOriginal": StringType(),
    "anoLancamento": DateType(),
    "tempoMinutos": IntegerType(),
    "genero": StringType(),
    "notaMedia": DoubleType(),
    "numeroVotos": IntegerType(),
    "generoArtista": StringType(),
    "personagem": StringType(),
    "nomeArtista": StringType()
}

# Definição do esquema para series
series_schema = {
    "id": StringType(),
    "tituloOriginal": StringType(),
    "anoLancamento": DateType(),
    "tempoMinutos": IntegerType(),
    "genero": StringType(),
    "notaMedia": DoubleType(),
    "numeroVotos": IntegerType(),
    "generoArtista": StringType(),
    "personagem": StringType(),
    "nomeArtista": StringType()
}

# Processa os arquivos de movies e series
adjust_schema_and_save(movies_input_path, f"{output_path}/csv/movies", movies_schema)
adjust_schema_and_save(series_input_path, f"{output_path}/csv/series", series_schema)

# Finaliza o job
job.commit()
