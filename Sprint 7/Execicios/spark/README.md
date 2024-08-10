# EXECERCICIOS DE SPARK

## Etapa 1: Realizar o pull da imagem

```sh
docker pull jupyter/all-spark-notebook
```

## Etapa 2: Criar e iniciar o container

```sh
docker run -it -p 8888:8888 jupyter/all-spark-notebook

```

## Etapa 3: Acessar o terminal interativo do Spark

```sh
docker exec -it <container_id> /bin/bash
pyspark
```

## Etapa 4: Comandos Spark para contar ocorrências de palavras

```sh
wget README.md
```

## Inicializar SparkContext

```sh
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)
```

## Ler o arquivo README.md

```sh
text_file = sc.textFile("README.md")
```

## Dividir as linhas em palavras

```sh
words = text_file.flatMap(lambda line: line.split(" "))
```

## Criar pares (palavra, 1)

```sh
word_pairs = words.map(lambda word: (word, 1))
```

## Contar as ocorrências de cada palavra

```sh
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
```

## Coletar e exibir os resultados

```sh
for word, count in word_counts.collect():
    print(f"{word}: {count}")

```

![RESPOSTA DO SCRIPT](/Sprint%207/Evidencias/SPARK)
