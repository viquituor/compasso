--PRIMEIROA EXERCICIOS 
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
--E07
--E08
--E09 
--E10