import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Captura dos argumentos
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Processamento dos arquivos CSV

# Processamento do movies.csv
movies_df = glueContext.create_dynamic_frame.from_catalog(
    database="db-csv",
    table_name="movies_csv",
    transformation_ctx="movies_df"
).toDF()

# Exemplo de transformação
movies_df = movies_df.withColumnRenamed("id", "id")
movies_df = movies_df.withColumnRenamed("tituloPincipal", "tituloPincipal")
movies_df = movies_df.withColumnRenamed("tituloOriginal", "tituloOriginal")
movies_df = movies_df.withColumnRenamed("anoLancamento", "anoLancamento")
movies_df = movies_df.withColumnRenamed("tempoMinutos", "tempoMinutos")
movies_df = movies_df.withColumnRenamed("genero", "genero")
movies_df = movies_df.withColumnRenamed("notaMedia", "notaMedia")
movies_df = movies_df.withColumnRenamed("numeroVotos", "numeroVotos")
movies_df = movies_df.withColumnRenamed("generoArtista", "generoArtista")
movies_df = movies_df.withColumnRenamed("personagem", "personagem")
movies_df = movies_df.withColumnRenamed("nomeArtista", "nomeArtista")
movies_df = movies_df.withColumnRenamed("anoNascimento", "anoNascimento")
movies_df = movies_df.withColumnRenamed("anoFalecimento", "anoFalecimento")
movies_df = movies_df.withColumnRenamed("profissao", "profissao")
movies_df = movies_df.withColumnRenamed("titulosMaisConhecidos", "titulosMaisConhecidos")

# Gravação em Parquet na Trusted Zone
movies_dynamic_frame = DynamicFrame.fromDF(movies_df, glueContext, "movies_dynamic_frame")
glueContext.write_dynamic_frame.from_options(
    frame=movies_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://projeto-pb/Trusted/movies"},
    format="parquet"
)

# Processamento do series.csv
series_df = glueContext.create_dynamic_frame.from_catalog(
    database="db-csv",
    table_name="series-csv",
    transformation_ctx="series_df"
).toDF()

series_df = series_df.withColumnRenamed("id", "id")
series_df = series_df.withColumnRenamed("tituloPincipal", "tituloPincipal")
series_df = series_df.withColumnRenamed("tituloOriginal", "tituloOriginal")
series_df = series_df.withColumnRenamed("anoLancamento", "anoLancamento")
series_df = series_df.withColumnRenamed("tempoMinutos", "tempoMinutos")
series_df = series_df.withColumnRenamed("genero", "genero")
series_df = series_df.withColumnRenamed("notaMedia", "notaMedia")
series_df = series_df.withColumnRenamed("numeroVotos", "numeroVotos")
series_df = series_df.withColumnRenamed("generoArtista", "generoArtista")
series_df = series_df.withColumnRenamed("personagem", "personagem")
series_df = series_df.withColumnRenamed("nomeArtista", "nomeArtista")
series_df = series_df.withColumnRenamed("anoNascimento", "anoNascimento")
series_df = series_df.withColumnRenamed("anoFalecimento", "anoFalecimento")
series_df = series_df.withColumnRenamed("profissao", "profissao")
series_df = series_df.withColumnRenamed("titulosMaisConhecidos", "titulosMaisConhecidos")

# Gravação em Parquet na Trusted Zone
series_dynamic_frame = DynamicFrame.fromDF(series_df, glueContext, "series_dynamic_frame")
glueContext.write_dynamic_frame.from_options(
    frame=series_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://projeto-pb/Trusted/series"},
    format="parquet"
)

job.commit()
