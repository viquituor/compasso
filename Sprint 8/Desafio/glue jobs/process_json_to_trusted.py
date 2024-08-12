import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Captura dos argumentos
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicialização do contexto Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Processamento dos arquivos JSON de filmes

try:
    # Leitura dos dados dos filmes do Glue Catalog
    movies_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database="db-json",
        table_name="movies-json",
        transformation_ctx="movies_dynamic_frame"
    )
    
    # Converte para DataFrame do Spark para transformação
    movies_df = movies_dynamic_frame.toDF()

    # Renomeia as colunas conforme necessário
    movies_df = movies_df.withColumnRenamed("id", "id")
    movies_df = movies_df.withColumnRenamed("title", "tituloPincipal")
    movies_df = movies_df.withColumnRenamed("release_date", "anoLancamento")
    movies_df = movies_df.withColumnRenamed("popularity", "popularidade")
    movies_df = movies_df.withColumnRenamed("vote_average", "notaMedia")
    movies_df = movies_df.withColumnRenamed("runtime", "tempoMinutos")

    # Converte de volta para DynamicFrame para gravação
    movies_dynamic_frame = DynamicFrame.fromDF(movies_df, glueContext, "movies_dynamic_frame")
    
    # Gravação em Parquet na Trusted Zone
    glueContext.write_dynamic_frame.from_options(
        frame=movies_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": "s3://projeto-pb/Trusted/json/movies/",
            "partitionKeys": []  # Se não precisar de particionamento, deixe a lista vazia
        },
        format="parquet"
    )
except Exception as e:
    print(f"Erro ao processar movies.json: {e}")

# Processamento do JSON de séries

try:
    # Leitura dos dados das séries do Glue Catalog
    series_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database="db-json",
        table_name="series-json",
        transformation_ctx="series_dynamic_frame"
    )
    
    # Converte para DataFrame do Spark para transformação
    series_df = series_dynamic_frame.toDF()

    # Renomeia as colunas conforme necessário
    series_df = series_df.withColumnRenamed("id", "id")
    series_df = series_df.withColumnRenamed("name", "tituloPincipal")
    series_df = series_df.withColumnRenamed("first_air_date", "anoLancamento")
    series_df = series_df.withColumnRenamed("popularity", "popularidade")
    series_df = series_df.withColumnRenamed("vote_average", "notaMedia")

    # Converte de volta para DynamicFrame para gravação
    series_dynamic_frame = DynamicFrame.fromDF(series_df, glueContext, "series_dynamic_frame")
    
    # Gravação em Parquet na Trusted Zone
    glueContext.write_dynamic_frame.from_options(
        frame=series_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": "s3://projeto-pb/Trusted/json/series/",
            "partitionKeys": []  # Se não precisar de particionamento, deixe a lista vazia
        },
        format="parquet"
    )
except Exception as e:
    print(f"Erro ao processar series.json: {e}")

# Finaliza o job
job.commit()
