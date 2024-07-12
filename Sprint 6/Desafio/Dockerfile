# Usar imagem base oficial do Python
FROM python:3.9

# Configurar diretório de trabalho
WORKDIR /app

# Copiar requisitos e script para o contêiner
COPY requirements.txt requirements.txt
COPY script.py script.py


# Copiar os arquivos CSV para o contêiner
COPY movies.csv movies.csv
COPY series.csv series.csv

# Instalar dependências
RUN pip install -r requirements.txt

# Comando para rodar o script Python
CMD ["python", "script.py"]
