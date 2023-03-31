import psycopg2

connection = psycopg2.connect(
        host="localhost",
        database="TPC-H",
        user="postgres",
        password="Drogba11")

def helper_function(string_query):
    
    cursor = connection.cursor()

    cursor.execute(string_query)

    records = cursor.fetchall()

    print(records)

string_query='EXPLAIN (ANALYZE, FORMAT JSON) select * from region;'

helper_function(string_query)
