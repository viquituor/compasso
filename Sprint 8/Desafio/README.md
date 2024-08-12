# Desafio de Processamento de Dados de Filmes e Séries

## Objetivo

O objetivo deste desafio é praticar o uso do AWS Glue com Apache Spark para processar dados de filmes e séries, integrando dados das camadas Raw e Trusted em um data lake. A meta é transformar e padronizar os dados, tornando-os disponíveis para análise através do AWS Athena.

## Processamento de Dados

O processamento foi dividido em dois jobs no AWS Glue:

### Job 1: Processamento de Dados CSV

Objetivo: Processar arquivos CSV da camada Raw e armazenar os dados na camada Trusted no formato Parquet.
[CODIGO DO SCRIPT DE CSV](/Sprint%208/Desafio/glue%20jobs/process_csv_to_trusted.py)

### Job 2: Processamento de Dados JSON

Objetivo: Processar arquivos JSON da camada Raw e armazenar os dados na camada Trusted no formato Parquet, particionados por data.
[CODIGO DO SCRIPT DE JSON](/Sprint%208/Desafio/glue%20jobs/process_json_to_trusted.py)


## Evidencias

Código Python:

![Realizaçao do RUN do scrip de CSV](/Sprint%208/Desafio/Evidencias/csv-sucesso.jpeg)
![Realizaçao do RUN do scrip de JSON](/Sprint%208/Desafio/Evidencias/json-sucesso.jpeg)
