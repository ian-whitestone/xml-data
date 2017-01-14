import general_utils as Ugen
import psycopg2


def db_connect():
    conn = psycopg2.connect(host='localhost',port=5432,database='nba',user='whitesi')
    return conn


def execute_query(conn, query, data, multiple=False):
    cur = conn.cursor()
    if multiple:  # data is a list of tuples
        cur.executemany(query, data)
    else:  # data is a single tuple
        cur.execute(query, data)
    conn.commit()
    cur.close()
    return


def select_query(conn, query, data=False,cols=False):
    cur = conn.cursor()
    if data:  # data is a single tuple
        if not isinstance(data, tuple):
            data = (data,)
        cur.execute(query, data)
        resultset = cur.fetchall()
    else:
        cur.execute(query)
        resultset = cur.fetchall()
    if cols:
        colnames = tuple([desc[0] for desc in cur.description])
        return [colnames]+resultset
    cur.close()
    return resultset
