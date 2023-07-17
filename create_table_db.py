
import psycopg2
from queries_db import * 
from Connect import * 
conn,cur = connect_postgres()
"""for i in create_table_list:
    cur.execute(i)
    conn.commit()"""

for table in create_table_list:
    cur.execute(table)
    print(f"already create table {table}")
    conn.commit()