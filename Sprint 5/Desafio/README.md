# DESAFIO DE AGREGAÇÃO

## DESAIO DE CONSULTA EM SQL PARA UMA BASE DE DADOS DO GOVERNO BRASILEIRO

```SQL
SELECT  
    DATA_EXIBICAO,
    LOWER(PAIS_OBRA) AS lower_pais_obra,   
    CASE WHEN PAIS_OBRA = 'BRASIL' THEN 'NACIONAL' ELSE 'INTERNACIONAL' END AS obras_nacionais, 
    TITULO_ORIGINAL, 
    TITULO_BRASIL, 
    PAIS_OBRA, 
    PUBLICO
FROM 
    S3Object
WHERE 
    PUBLICO < '99' AND PUBLICO > '20'
    limit 100
```

### CONSULTA COM TODOS OS REQUISITOS MAS A AGREGAÇÃO NÃO FOI POSSIVEL INCLUIR NA CONSULTA

## CONSULTA DE AGREGAÇÃO PARA COMPRIMENTO DE REQ UISITOS DO DESAFIO

```SQL
SELECT COUNT(*) FROM S3Object
```

### [CONSULTA SQL](/Sprint%205/Desafio/aws.py)

### [CONSLTA DE AGREGAÇÃO](/Sprint%205/Desafio/agregacao.py)
