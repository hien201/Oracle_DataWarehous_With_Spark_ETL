

import psycopg2


def connect_postgres():
    conn = psycopg2.connect("host = 127.0.0.1 dbname = Immigration user = postgres password = postgres")
    cur = conn.cursor()

    return conn,cur 