mkdir vendas

cp dados_de_vendas.csv vendas/ 

mkdir vendas/backup

data=$(date +%Y%m%d)

cp vendas/dados_de_vendas.csv vendas/backup/backup-dados-"$data".csv

echo "data do sistema operacional: $(date '+%Y/%M/%D %H:%M')" >  vendas/backup/relatorio.txt
echo "Data do primeiro registro de venda:" $(head -n 1 vendas/dados_de_vendas.csv | cut -d ',' -f 5) >> vendas/backup/relatorio.txt 
echo "Data do último registro de venda:" $(tail -n 1 vendas/dados_de_vendas.csv | cut -d ',' -f 5) >> vendas/backup/relatorio.txt 
echo "Quantidade total de itens diferentes vendidos:" $(cut -d ',' -f 2 vendas/dados_de_vendas.csv | sort | uniq | wc -l) >> vendas/backup/relatorio.txt 
echo "Primeiras 10 linhas do arquivo:" >> vendas/backup/relatorio.txt 
head vendas/backup/backup-dados-"$data".csv >> vendas/backup/relatorio.txt

zip -j vendas/backup/backup-dados-"$data".zip vendas/backup/backup-dados-"$data".csv

rm vendas/backup/backup-dados-"$data".csv vendas/dados_de_vendas.csv

