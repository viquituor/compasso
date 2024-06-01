# Função para ler o arquivo CSV
def read_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    header = lines[0].strip().split(',')
    data = [line.strip().split(',') for line in lines[1:]]
    return header, data

# Função para remover espaços extras dos dados
def clean_data(data):
    cleaned_data = []
    for row in data:
        cleaned_row = [item.strip().replace('"', '') for item in row]
        cleaned_data.append(cleaned_row)
    return cleaned_data

# Função para salvar dados em arquivos de texto
def save_to_file(file_path, content):
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)

# Caminho para o arquivo CSV e os arquivos de saída
csv_file = r'C:\\Users\\User\\OneDrive\\Área de Trabalho\\trainee-repo-template\\Sprint 3\\Exercicios\\actors.csv'
output_files = [f'C:\\Users\\User\\OneDrive\\Área de Trabalho\\trainee-repo-template\\Sprint 3\\Exercicios\\etapa-{i}.txt' for i in range(1, 6)]

# Lendo e limpando o arquivo CSV
header, data = read_csv(csv_file)
data = clean_data(data)

# Depuração: Imprimir as primeiras linhas do arquivo CSV
print("Header:", header)
print("First 5 rows of data:", data[:5])

# Funções para cada etapa

# Etapa 1: Ator/atriz com maior número de filmes
def etapa_1(data):
    max_movies = 0
    actor_name = ''
    for row in data:
        try:
            num_movies = int(row[2])
            if num_movies > max_movies:
                max_movies = num_movies
                actor_name = row[0]
        except ValueError as e:
            print(f"Error converting {row[2]} to int: {e}")
    return f'{actor_name} - {max_movies}'

# Etapa 2: Média de receita bruta dos principais filmes
def etapa_2(data):
    total_gross = 0
    count = 0
    for row in data:
        try:
            gross = float(row[5])
            total_gross += gross
            count += 1
        except ValueError as e:
            print(f"Error converting {row[5]} to float: {e}")
    average_gross = total_gross / count
    return f'{average_gross:.2f}'

# Etapa 3: Ator/atriz com a maior média de receita por filme
def etapa_3(data):
    max_avg_gross = 0
    actor_name = ''
    for row in data:
        try:
            avg_gross = float(row[3])
            if avg_gross > max_avg_gross:
                max_avg_gross = avg_gross
                actor_name = row[0]
        except ValueError as e:
            print(f"Error converting {row[3]} to float: {e}")
    return f'{actor_name} - {max_avg_gross:.2f}'

# Etapa 4: Contagem de aparições dos filmes de maior bilheteria
def etapa_4(data):
    movie_counts = {}
    for row in data:
        movie = row[4]
        if movie in movie_counts:
            movie_counts[movie] += 1
        else:
            movie_counts[movie] = 1
    sorted_movies = sorted(movie_counts.items(), key=lambda x: (-x[1], x[0]))
    result = '\n'.join([f'O filme {movie} aparece {count} vez(es) no dataset' for movie, count in sorted_movies])
    return result

# Etapa 5: Lista dos atores ordenada pela receita bruta de bilheteria
def etapa_5(data):
    actors_gross = []
    for row in data:
        try:
            actor = row[0]
            total_gross = float(row[1])
            actors_gross.append((actor, total_gross))
        except ValueError as e:
            print(f"Error converting {row[1]} to float: {e}")
    sorted_actors = sorted(actors_gross, key=lambda x: -x[1])
    result = '\n'.join([f'{actor} - {gross:.2f}' for actor, gross in sorted_actors])
    return result

# Processando cada etapa e salvando os resultados nos arquivos correspondentes
etapa_results = [
    etapa_1(data),
    etapa_2(data),
    etapa_3(data),
    etapa_4(data),
    etapa_5(data)
]

for i, result in enumerate(etapa_results):
    save_to_file(output_files[i], result)

# Outputs para conferência
etapa_results
