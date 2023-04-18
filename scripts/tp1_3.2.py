'''
Noah Diunkz Oliveira Maia
github: /diunkz
linkedin: /in/diunkz
'''

import psycopg2
import re
from datetime import datetime
from psycopg2.extras import execute_values

OCORRENCIAS = 10000

now = datetime.now()
print(f'{now.hour}:{now.minute}:{now.second}')

conn = psycopg2.connect(host='localhost', port='5432', user="postgres", password="postgres")
conn.autocommit = True

cur = conn.cursor()

# limpando o banco caso as tabelas existam
sql = "DROP TABLE IF EXISTS categories, productcategories, productreviews, products, similarproducts;"

cur.execute(sql)

# criação das tabelas
sql =  "CREATE TABLE Products (\
            Id INTEGER PRIMARY KEY,\
            Asin VARCHAR(10),\
            Title VARCHAR(500),\
            GroupName VARCHAR(12),\
            Salesrank INTEGER,\
            UNIQUE(Asin)\
        );\
        \
        CREATE TABLE SimilarProducts (\
            ProductId INTEGER REFERENCES Products(Id),\
            SimilarProductId VARCHAR(10),\
            UNIQUE (ProductId, SimilarProductId)\
        );\
        CREATE TABLE Categories (\
            CategoryName VARCHAR(255),\
            CategoryId INTEGER PRIMARY KEY\
        );\
        CREATE TABLE ProductCategories (\
            ProductId INTEGER REFERENCES Products(Id),\
            CodeCategory INTEGER REFERENCES Categories(CategoryId),\
            UNIQUE(ProductId, CodeCategory)\
        );\
        CREATE TABLE ProductReviews (\
            ProductId INTEGER REFERENCES Products(Id),\
            ReviewDate DATE,\
            CustomerId VARCHAR(25),\
            Rating INTEGER,\
            Votes INTEGER,\
            Helpful INTEGER\
        );"

cur.execute(sql)

def insert_products(arq):
    now = datetime.now()
    print(f'products start -> {now.hour}:{now.minute}:{now.second}')

    insert_products = []
    linha = arq.readline()

    while linha:
        if 'Id:   ' in linha:

            id = linha.split("Id:   ")[-1].replace('\n','')
            
            linha = arq.readline()
            asin = linha.split("ASIN: ")[-1].replace('\n','')

            linha = arq.readline()
            if 'discontinued product' in linha:
                linha = arq.readline()
            else:
                title = linha.split("title: ")[-1].replace('\n','')
                title = title.replace("'", "\'\'")

                linha = arq.readline()
                group = linha.split("group: ")[-1].replace('\n','')
                
                linha = arq.readline()
                salesrank = linha.split("salesrank: ")[-1].replace('\n','')

                insert_products.append((id, asin, title, group, salesrank))
        linha = arq.readline()

    query = f"INSERT INTO Products (Id, Asin, Title, GroupName, Salesrank)\
              VALUES %s;"

    execute_values(cur, query, insert_products)

    del insert_products

    now = datetime.now()
    print(f'products end -> {now.hour}:{now.minute}:{now.second}')

def insert_similar_products(arq):
    now = datetime.now()
    print(f'similar products start -> {now.hour}:{now.minute}:{now.second}')

    insert_similar_products = []
    linha = arq.readline()

    while linha:
        if 'Id:   ' in linha:
            id = linha.split("Id:   ")[-1].replace('\n','')
        if 'similar: ' in linha:
            linha = linha.replace('  similar: ', '')
            linha = linha.replace('\n', '')
            linha = linha.split('  ')
            linha = linha[1:]

            for x in linha:
                insert_similar_products.append((id,x))

        linha = arq.readline()
    
    query = f"INSERT INTO SimilarProducts (ProductId, SimilarProductId)\
              VALUES %s;"

    execute_values(cur, query, insert_similar_products)

    del insert_similar_products

    now = datetime.now()
    print(f'similar products end -> {now.hour}:{now.minute}:{now.second}')

def insert_categories(arq):
    now = datetime.now()
    print(f'categories start -> {now.hour}:{now.minute}:{now.second}')
    
    linha = arq.readline()
    insert_categories = []

    while linha:
        if 'categories:' in linha:
            linha = arq.readline()
            categories = []
            while 'reviews:' not in linha:
                linha = linha.replace('\n', '')
                linha = linha.replace(' ', '')

                # separando palavra e número das categorias
                linha = linha[1:]
                linha = linha.split('|')

                for x in linha:
                    regex = r"\[\d+\]"
                    numero = re.findall(regex, x)[0][1:-1]
                    palavra = re.sub(regex, '', x)
                    if tuple((palavra, numero)) not in categories:
                        categories.append((palavra, numero))
                        
                linha = arq.readline()

            for x in range(len(categories)):
                z = None if categories[x][0] == '' else categories[x][0]
                insert_categories.append((z, categories[x][1]))

            if len(insert_categories) > OCORRENCIAS:
                query = f'INSERT INTO Categories (CategoryName, CategoryId)\
                          VALUES %s ON CONFLICT DO NOTHING;'

                execute_values(cur, query, insert_categories)

                del categories
                del insert_categories
                insert_categories = []
    
        linha = arq.readline()

    
    query = f'INSERT INTO Categories (CategoryName, CategoryId)\
                VALUES %s ON CONFLICT DO NOTHING;'

    execute_values(cur, query, insert_categories)

    del categories
    del insert_categories

    now = datetime.now()
    print(f'categories end -> {now.hour}:{now.minute}:{now.second}')

def insert_product_categories(arq):
    now = datetime.now()
    print(f'product categories start -> {now.hour}:{now.minute}:{now.second}')

    linha = arq.readline()
    insert_product_categories = []
    
    while linha:
        if 'Id:   ' in linha:
            id = linha.split("Id:   ")[-1].replace('\n','')
            linha = arq.readline()
        if 'categories:' in linha:
            linha = arq.readline()
            categories = []
            while 'reviews:' not in linha:
                linha = linha.replace('\n', '')
                linha = linha.replace(' ', '')

                # separando palavra e número das categorias
                linha = linha[1:]
                linha = linha.split('|')

                for x in linha:
                    regex = r"\[\d+\]"
                    numero = re.findall(regex, x)[0][1:-1]
                    palavra = re.sub(regex, '', x)
                    if tuple((palavra, numero)) not in categories:
                        categories.append((palavra, numero))
                        
                linha = arq.readline()

            for x in range(len(categories)):
                insert_product_categories.append((id, categories[x][1]))

            if len(insert_product_categories) > OCORRENCIAS:
                query = f"INSERT INTO ProductCategories (ProductId, CodeCategory)\
                          VALUES %s;"

                execute_values(cur, query, insert_product_categories)

                del insert_product_categories
                insert_product_categories = []
    
        linha = arq.readline()

    query = f"INSERT INTO ProductCategories (ProductId, CodeCategory)\
              VALUES %s;"

    execute_values(cur, query, insert_product_categories)
    
    del categories
    del insert_product_categories

    now = datetime.now()
    print(f'product categories end -> {now.hour}:{now.minute}:{now.second}')

def insert_product_reviews(arq):
    now = datetime.now()
    print(f'product reviews start-> {now.hour}:{now.minute}:{now.second}')
    
    linha = arq.readline()
    insert_product_reviews = []

    while linha:
        if 'Id:   ' in linha:
            id = linha.split("Id:   ")[-1].replace('\n','')

        if 'reviews:' in linha:
            reviews = []
            linha = arq.readline()
            while 'helpful:' in linha:
                data = re.search(r'^\s*(.*?)\s*cutomer:', linha).group(1)
                customer = re.search(r'cutomer:\s*(\S+)\s*r', linha).group(1)
                rating = re.search(r'rating:\s*(.*?)\s*votes:', linha).group(1)
                votes = re.search(r'votes:\s*(.*?)\s*helpful:', linha).group(1)
                helpful = re.search(r'helpful:\s*(.*)$', linha).group(1)

                reviews.append((data, customer, rating, votes, helpful))
                linha = arq.readline()

            for x in reviews:
                insert_product_reviews.append((id, x[0], x[1], x[2], x[3], x[4]))
            
            del reviews

            if len(insert_product_reviews) > OCORRENCIAS:
                query = f"INSERT INTO ProductReviews (ProductId, ReviewDate, CustomerId, Rating, Votes, Helpful)\
                          VALUES %s;"

                execute_values(cur, query, insert_product_reviews)

                del insert_product_reviews
                insert_product_reviews = []

        linha = arq.readline()

    query = f"INSERT INTO ProductReviews (ProductId, ReviewDate, CustomerId, Rating, Votes, Helpful)\
                VALUES %s;"

    execute_values(cur, query, insert_product_reviews)

    del insert_product_reviews

    now = datetime.now()
    print(f'product reviews end -> {now.hour}:{now.minute}:{now.second}')

# leitura do arquivo
file = "../amazon-meta.txt"

arq = open(file)
insert_products(arq)
arq = open(file)
insert_similar_products(arq)
arq = open(file)
insert_categories(arq)
arq = open(file)
insert_product_categories(arq)
arq = open(file)
insert_product_reviews(arq)

now = datetime.now()
print(f'{now.hour}:{now.minute}:{now.second}')
