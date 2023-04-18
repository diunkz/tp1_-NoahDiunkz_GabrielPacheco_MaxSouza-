'''
Noah Diunkz Oliveira Maia
github: /diunkz
linkedin: /in/diunkz
'''

import psycopg2
from os import system, name

conn = psycopg2.connect(host='localhost', port='5432', user="postgres", password="postgres")

conn.autocommit = True

cur = conn.cursor()

def query(sql):
    try:
        cur.execute(sql)
    except Exception as e:
        return (f"Ocorreu um erro: {e}")
    return cur.fetchall()

entrada = ''

print()
print("Trabalho 1 de BD!")
print()

possiveis_entradas = [str(x) for x in range(1,10)]
possiveis_entradas.remove('8')

while entrada != '0':
    system('cls' if name == 'nt' else 'clear')
    print("Qual questão deseja visualizar a solução? (1-7)")
    print("\t\tInsira 0 para sair do programa.")
    print("\t\tInsira 9 para inserir sua própria query.")
    entrada = input("-> ")

    if entrada == '1':
        system('cls' if name == 'nt' else 'clear')
        print('Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação')
        print()
        entry = input('Insira o ASIN -> ')
        print()
        print('------------------------------------------------------------------------------------')
        print()

        print('5 comentários mais úteis e com maior avaliação:')
        print()
        exec = query(f"(SELECT * FROM ProductReviews pr\
                        WHERE pr.ProductId = (SELECT p.Id FROM Products p WHERE p.Asin = '{entry}')\
                        ORDER BY Rating DESC, Helpful DESC\
                        LIMIT 5);")
        
        print('productid - reviewdate - customerid - rating - votes - helpful')
        for x in exec:
            print(f'{x[0]} - {x[1].strftime("%d/%m/%Y")} - {x[2]} - {x[3]} - {x[4]} - {x[5]}')
        
        print()
        print('------------------------------------------------------------------------------------')
        print()

        print('5 comentários mais úteis e com menor avaliação')
        print()
        exec = query(f"(SELECT *\
                        FROM ProductReviews pr\
                        WHERE pr.ProductId = (SELECT p.Id FROM Products p WHERE p.Asin = '{entry}')\
                        ORDER BY Rating ASC, Helpful DESC\
                        LIMIT 5);")\
        
        print('productid - reviewdate - customerid - rating - votes - helpful')
        for x in exec:
            print(f'{x[0]} - {x[1].strftime("%d/%m/%Y")} - {x[2]} - {x[3]} - {x[4]} - {x[5]}')
        
    elif entrada == '2':
        system('cls' if name == 'nt' else 'clear')
        print('Dado um produto, listar os produtos similares com maiores vendas do que ele')
        print()
        entry = input('Insira o ASIN -> ')
        print()
        print('------------------------------------------------------------------------------------')
        print()
        print('Produtos similares com maiores vendas do que ele:')
        print()
        exec = query(f"SELECT sp.SimilarProductId, p.Title, p.Salesrank\
                       FROM SimilarProducts sp\
                       JOIN Products p ON sp.SimilarProductId = p.Asin\
                       WHERE sp.ProductId = (SELECT Id FROM Products WHERE ASIN = '{entry}')\
                       AND p.Salesrank < (SELECT Salesrank FROM Products WHERE ASIN = '{entry}')\
                       ORDER BY p.Salesrank ASC;")
        
        print('rank - title - sales')
        for x in exec:
            print(f'{x[0]} - {x[1]} - {x[2]}')
    
    elif entrada == '3':
        system('cls' if name == 'nt' else 'clear')
        print('Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada ')
        print()
        entry = input('Insira o ASIN -> ')
        print()
        print('------------------------------------------------------------------------------------')
        print()
        print('Produtos similares com maiores vendas do que ele:')
        print()
        exec = query(f"SELECT ReviewDate, ROUND(AVG(Rating),3) AS AvgRating\
                       FROM ProductReviews\
                       WHERE ProductId = (SELECT Id FROM Products WHERE ASIN = '{entry}')\
                       GROUP BY ReviewDate\
                       ORDER BY ReviewDate;")
        
        print('reviewdate - avg(rating)')
        
        for x in exec:
            print(f'{x[0]} - {x[1]}')    
    elif entrada == '4':
        system('cls' if name == 'nt' else 'clear')
        print('Listar os 10 produtos líderes de venda em cada grupo de produtos.')
        print()
        print('------------------------------------------------------------------------------------')
        print()
        exec = query(f"SELECT p.GroupName, p.Title, p.Salesrank\
                       FROM (\
                       SELECT GroupName, Title, Salesrank, \
                       dense_rank() OVER (PARTITION BY GroupName ORDER BY Salesrank) AS rank\
                       FROM Products\
                       WHERE Salesrank > 0\
                       ) p\
                       WHERE p.rank <= 10;")
        
        print('groupname - title - salesrank')
        for x in exec:
            print(f'{x[0]} - {x[1]} - {x[2]}')
    
    elif entrada == '5':
        system('cls' if name == 'nt' else 'clear')
        print('Listar os 10 produtos com a maior média de avaliações úteis positivas por produto:')
        print()
        print('------------------------------------------------------------------------------------')
        print()
        exec = query(f"SELECT Products.Id, Products.Title, ROUND(AVG(ProductReviews.Helpful),3) as AvgHelpful\
                       FROM Products\
                       INNER JOIN ProductReviews ON Products.Id = ProductReviews.ProductId\
                       WHERE ProductReviews.Helpful > 0\
                       GROUP BY Products.Id, Products.Title\
                       ORDER BY AvgHelpful DESC\
                       LIMIT 10;")
        
        print('id - title - avg(helpful)')
        for x in exec:
            print(f'{x[0]} - {x[1]} - {x[2]}')
    elif entrada == '6':
        system('cls' if name == 'nt' else 'clear')
        print('Listar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto')
        print()
        print('------------------------------------------------------------------------------------')
        print()
        exec = query(f"SELECT Categories.CategoryName, AVG(ProductReviews.Helpful) AS AvgHelpful\
                       FROM ProductReviews\
                       INNER JOIN Products ON ProductReviews.ProductId = Products.Id\
                       INNER JOIN ProductCategories ON Products.Id = ProductCategories.ProductId\
                       INNER JOIN Categories ON ProductCategories.CodeCategory = Categories.CategoryId\
                       GROUP BY Categories.CategoryName\
                       ORDER BY AvgHelpful DESC\
                       LIMIT 5;")
        
        print('categoryname - avg(helpful)')
        for x in exec:
            print(f'{x[0]} - {x[1]}')
    elif entrada == '7':
        system('cls' if name == 'nt' else 'clear')
        print('Listar os 10 clientes que mais fizeram comentários por grupo de produto:')
        print()
        entry = input('Insira o GroupName -> ')
        print()
        print('------------------------------------------------------------------------------------')
        print()
        exec = query(f"SELECT p.GroupName, pr.CustomerId, COUNT(*) AS NumReviews\
                       FROM Products p \
                       JOIN ProductReviews pr ON p.Id = pr.ProductId\
                       WHERE p.GroupName = '{entry}'\
                       GROUP BY p.GroupName, pr.CustomerId \
                       ORDER BY NumReviews DESC\
                       LIMIT 10;")
        
        print('groupname - customerid - numreviews')
        for x in exec:
            print(f'{x[0]} - {x[1]}- {x[2]}')
    elif entrada == '9':
        system('cls' if name == 'nt' else 'clear')
        entry = input("Insira sua Query -> ")
        print()
        print('------------------------------------------------------------------------------------')
        print()
        exec = query(entry)

        if str(exec):
            print(exec)
        else:
            for x in exec:
                print(x)
    if entrada in possiveis_entradas:
        print()
        print('------------------------------------------------------------------------------------')
        print()
        input('pressione enter para continuar...')