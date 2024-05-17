CREATE TABLE CLIENTE (
    idCliente INT PRIMARY KEY,
    nomeCliente VARCHAR(255),
    cidadeCliente VARCHAR(255),
    estadoCliente VARCHAR(255),
    paisCliente VARCHAR(255)
);

INSERT INTO  CLIENTE (idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente)
select DISTINCT  idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente
from tb_locacao 

CREATE TABLE CARRO (
    idCarro INT PRIMARY KEY,
    kmCarro INT,
    classiCarro VARCHAR(255),
    marcaCarro VARCHAR(255),
    modeloCarro VARCHAR(255),
    anoCarro INT
);

INSERT INTO CARRO (idCarro, kmCarro, classiCarro, marcaCarro, modeloCarro, anoCarro)
SELECT DISTINCT idCarro, kmCarro, classiCarro, marcaCarro, modeloCarro, anoCarro
FROM tb_locacao
group by idcarro



CREATE TABLE VENDEDOR (
    idVendedor INT PRIMARY KEY,
    nomeVendedor VARCHAR(255),
    sexoVendedor INT,
    estadoVendedor VARCHAR(255)
); 

INSERT into VENDEDOR (idVendedor,nomeVendedor,sexoVendedor,estadoVendedor)
select DISTINCT idVendedor ,nomeVendedor,sexoVendedor ,estadoVendedor 
from tb_locacao


CREATE TABLE COMBUSTIVEL (
    idCombustivel INT PRIMARY KEY,
    tipoCombustivel VARCHAR(255),
    idCarro INT,
    FOREIGN KEY (idCarro) REFERENCES CARRO (idCarro) ON DELETE RESTRICT
);

INSERT into COMBUSTIVEL (idCombustivel,tipoCombustivel,idCarro)
select DISTINCT  idCombustivel , tipoCombustivel ,idCarro 
from tb_locacao 
GROUP BY idcombustivel 

CREATE TABLE LOCACAO (
    horaEntrega TIME,
    dataEntrega DATE,
    vlrDiaria DECIMAL,
    qtdDiaria INT,
    dataLocacao DATE,
    horaLocacao TIME,
    idCliente INT,
    idCarro INT,
    idVendedor INT,
    idLocacao INT,
    FOREIGN KEY (idCliente) REFERENCES CLIENTE (idCliente),
    FOREIGN KEY (idCarro) REFERENCES CARRO (idCarro),
    FOREIGN KEY (idVendedor) REFERENCES VENDEDOR (idVendedor)
);

INSERT into LOCACAO (horaEntrega,dataEntrega,vlrDiaria,qtdDiaria,dataLocacao,horaLocacao,idCliente,idCarro,idVendedor,idLocacao)
select DISTINCT horaEntrega, dataEntrega, vlrDiaria, qtdDiaria, dataLocacao, horaLocacao, idCliente, idCarro, idVendedor, idLocacao
from tb_locacao 