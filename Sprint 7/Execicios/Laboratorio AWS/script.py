import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, upper, desc

# Parâmetros do job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Leitura do arquivo CSV no S3
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
).toDF()

# Imprimir o schema do dataframe
df.printSchema()

# Alterar a caixa dos valores da coluna 'nome' para maiúsculo
df_upper = df.withColumn("nome", upper(col("nome")))

# Imprimir a contagem de linhas presentes no dataframe
print(f"Total de linhas no dataframe: {df_upper.count()}")

# Contagem de nomes, agrupando pelos campos 'ano' e 'sexo', ordenado pelo ano mais recente primeiro
df_grouped = df_upper.groupBy("ano", "sexo").count().orderBy(desc("ano"))
df_grouped.show()

# Nome feminino com mais registros e em que ano ocorreu
feminino_top = df_upper.filter(df_upper.sexo == "F").groupBy("nome", "ano").count().orderBy(desc("count")).first()
print(f"Nome feminino com mais registros: {feminino_top['nome']}, Ano: {feminino_top['ano']}")

# Nome masculino com mais registros e em que ano ocorreu
masculino_top = df_upper.filter(df_upper.sexo == "M").groupBy("nome", "ano").count().orderBy(desc("count")).first()
print(f"Nome masculino com mais registros: {masculino_top['nome']}, Ano: {masculino_top['ano']}")

# Total de registros (masculinos e femininos) para cada ano (apenas as primeiras 10 linhas, ordenadas pelo ano)
total_por_ano = df_upper.groupBy("ano").count().orderBy("ano").limit(10)
total_por_ano.show()

# Escrever o conteúdo do dataframe com os valores de nome em maiúsculo no S3
df_dynamic = DynamicFrame.fromDF(df_upper, glueContext, "df_dynamic")

glueContext.write_dynamic_frame.from_options(
    frame = df_dynamic,
    connection_type = "s3",
    connection_options = {
        "path": f"{target_path}/frequencia_registro_nomes_eua",
        "partitionKeys": ["sexo", "ano"]
    },
    format = "json"
)

job.commit()
