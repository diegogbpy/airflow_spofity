"""
Consultar e extrair informações do Postgres, salvando arquivo em formato csv.
"""

import psycopg2 as db


def run_from_db_to_csv():

    conn_string = "dbname='' host='' user='' password=''"

    conn = db.connect(conn_string)
    cur = conn.cursor()

    query = "select * from minha_lista"

    cur.execute(query)

    print("Informacoes da Query")
    informacoes = [cur.fetchone()]

    for info in informacoes:
        print(info)

    f = open("/home/debian/Downloads/minha_lista_musicas.csv", "w")
    cur.copy_to(f, "minha_lista", sep=",")
    f.close()
