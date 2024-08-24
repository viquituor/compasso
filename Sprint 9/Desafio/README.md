# DESAFIO DA SPRINT 09

 foi preciso modificar os desafios das sprints passadas como adicionar o idioma nos dados da tmdb e aumentar o numero de dados.

## O DESAFIO CONTOU COM O PROCESSAMETO DOS DADOS DA CAMADA TRUST PARA A REFINED, ASSIM FOI FEIO AJUSTANDO OS TIPOS DE DADOS DE CADA TABELA, E TAMBEM FOI CRIADO A BASE DE DADOS NO GLUE CATALOG COM TODAS AS TABELAS PARA SER CONSULTADO NO ATHENA E USADO NO QUICKSIGHT

## PROCESSAMENTO DA CAMADA TRUSTED AJUSTANTO OS TIPOS DE DADOS COM JOBS DO GLUE STUDIO

![JOBS](/Sprint%209/Desafio/Evidencias/JOBS-GLUE-STUDIO.png)
[SCRIPT DO PROCESSAMENTO DOS ARQUIVOS LOCAIS](/Sprint%209/Desafio/Refined_local.py)
[SCRIPT DO PROCESSAMENTO DOS ARQUIVOS TMDB](/Sprint%209/Desafio/Refined_tmdb.py)

## CRIAÇAO DE BASE DE DADOS, TABELAS E COLUNAS

![TABELAS E BASE DE DADOS](/Sprint%209/Desafio/Evidencias/database-tables.png)
![COLUNAS DA TABELA LOCAL DE FILMES](/Sprint%209/Desafio/Evidencias/schemas-movies-local.png)
![COLUNAS DA TABELA LOCAL DE SERIES](/Sprint%209/Desafio/Evidencias/schemas-series-local.png)
![COLUNAS DA TABELA TMDB DE SERIES](/Sprint%209/Desafio/Evidencias/schemas.series-tmdb.png)
![COLUNAS DA TABELA TMDB DE FILMES](/Sprint%209/Desafio/Evidencias/schemas-movies-tmdb.png)
