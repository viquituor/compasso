--PRIMEIROS EXERCICIOS 
--caso de estudo BIBLIOTECA
--E01

SELECT  * 
from livro
where publicacao >= '2015/01/01'
order by cod desc
--E02

SELECT titulo, valor 
from livro
order by valor DESC 
limit 10

--E03

SELECT COUNT(livro.cod) AS quantidade, editora.nome AS nome,  endereco.estado, endereco.cidade 
FROM livro
JOIN editora ON livro.editora = codeditora
JOIN endereco on editora.endereco = codendereco
GROUP BY editora.nome;

--E04

SELECT autor.nome, autor.codautor ,autor.nascimento ,COUNT(livro.cod) AS quantidade
FROM livro
full JOIN autor ON livro.autor = codautor
GROUP BY autor.nome
order by autor.nome

--E05

SELECT autor.nome
FROM livro
JOIN autor ON livro.autor = codautor
join editora on livro.editora = codeditora
join endereco on editora.endereco = codendereco
WHERE estado <> 'PARANÁ' AND estado <> 'SANTA CATARINA' AND estado <> 'RIO GRANDE DO SUL'
GROUP BY autor.nome 
ORDER by autor.nome asc
--E06

SELECT autor.codautor, autor.nome,count(livro.cod) as quantidade_publicacoes
FROM autor
JOIN livro ON livro.autor = codautor
GROUP BY autor.nome 
ORDER by quantidade_publicacoes desc
limit 1

--E07
SELECT autor.nome
FROM autor
full JOIN livro ON livro.autor = codautor
GROUP BY autor.nome
HAVING count(livro.cod) < 1

--SEGUNDA PARTE DOS EXERCICIOS
--CASO DE ESTUDO LOJA

--E08

SELECT tbvendedor.cdvdd , nmvdd
FROM tbvendedor
full join tbvendas on tbvendas.cdvdd = tbvendedor.cdvdd 
where status = 'Concluído' 
GROUP by tbvendedor.cdvdd
HAVING count(*) = (
SELECT MAX(num_vendas)
from (
select COUNT(*) as num_vendas
from tbvendas
where status ='Concluído'
group by cdvdd)
)

--E09

SELECT cdpro, nmpro 
from tbvendas
where dtven > '2014/02/03' and dtven <'2018/02/02'
GROUP by cdpro
HAVING count(*)=(
SELECT MAX(mais_vendido)
from (
select COUNT(*) as mais_vendido
from tbvendas
GROUP by cdpro))

--E10

--E11

SELECT cdcli, nmcli,sum(qtd* vrunt) as gasto
from tbvendas
where status ='Concluído' 
group by cdcli
order by gasto desc
limit 1

--E12

SELECT  cddep, nmdep, dtnasc,sum(qtd * vrunt) as valor_total_vendas
from tbvendedor 
join tbdependente on tbdependente.cdvdd = tbvendedor.cdvdd
join tbvendas on tbvendas.cdvdd = tbvendedor.cdvdd 
where status = 'Concluído' 
GROUP by cddep 
ORDER by valor_total_vendas
limit 1

--E13

SELECT  cdpro, nmcanalvendas, nmpro, sum(qtd) as quantidade_vendas
from tbvendas
where status = 'Concluído' 
GROUP by nmcanalvendas, nmpro 
ORDER by quantidade_vendas 
limit 10

--E14

SELECT  estado , ROUND(avg(qtd*vrunt),2) as gastomedio
from tbvendas
where status = 'Concluído' 
GROUP by estado 
ORDER by gastomedio desc

--E15

SELECT  cdven 
from tbvendas
where deletado  = 1  

--E16

SELECT estado, nmpro, ROUND(avg(qtd),4) as quantidade_media
from tbvendas
where status ='Concluído' 
GROUP by estado, nmpro