import psycopg2

#realtby_data_table
def delete_table():
    conn = psycopg2.connect(dbname='pet_project_db', user='postgres',
                            password='changeme', host='localhost')
    cursor = conn.cursor()
    try:
        cursor.execute('drop table test')
    except psycopg2.errors.UndefinedTable:
        pass
