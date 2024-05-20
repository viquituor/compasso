 

CREATE view dim_CLIENTE AS
SELECT DISTINCT 
    idCliente as idcliente,
    nomeCliente as nome,
    cidadeCliente as cidade,
    estadoCliente as estado,
    paisCliente as pais
    from CLIENTE 


CREATE view dim_CARRO AS
SELECT DISTINCT 
    idCarro as idcarro,
    kmCarro as km,
    classiCarro as classi,
    marcaCarro as marca,
    modeloCarro as modelo,
    anoCarro as ano
    from CARRO 



CREATE view dim_VENDEDOR AS
SELECT DISTINCT 
    idVendedor as idVendedor,
    nomeVendedor as nome,
    sexoVendedor as sexo,
    estadoVendedor as estato
    from vendedor


CREATE view dim_COMBUSTIVEL AS
SELECT DISTINCT 
    idCombustivel as idCombustivel,
    tipoCombustivel as tipoCombustivel,
    idCarro as idCarro
    from COMBUSTIVEL c 
     


CREATE VIEW fact_LOCACAO AS
SELECT DISTINCT 
    idLocacao AS idLocacao,
    horaEntrega AS horaEntrega,
    dataEntrega AS dataEntrega,
    vlrDiaria AS vlrDiaria,
    qtdDiaria AS qtdDiaria,
    dataLocacao AS dataLocacao,
    horaLocacao AS horaLocacao,
    idCliente AS idCliente,
    idCarro AS idCarro,
    idVendedor AS idVendedor
FROM LOCACAO;
   
