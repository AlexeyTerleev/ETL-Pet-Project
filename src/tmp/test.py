import re
import psycopg2
conn = psycopg2.connect(dbname='pet_project_db', user='postgres',
                        password='changeme', host='localhost')
cursor = conn.cursor()

cursor.execute('SELECT * FROM realtby_data_table LIMIT 10')
records = cursor.fetchall()
for line in records:
    print(line)
cursor.close()
conn.close()