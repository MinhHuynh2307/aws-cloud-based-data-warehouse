import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drop tables if they exist.
    
    Args:
        cur  : a cursor
        conn : connection to the database
    
    Returns:
        Results from sql query execution
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging, fact, and dimension tables for star schema in Redshift
    
    Args:
        cur  : a cursor
        conn : connection to the database
    
    Returns:
        Results from sql query execution
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to Redshift database, get a cursor, then run drop_tables() and create_tables() functions
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    host     = config.get('DWH', 'HOST')
    dbname   = config.get('DWH', 'DWH_DB')
    user     = config.get('DWH', 'DWH_DB_USER')
    password = config.get('DWH', 'DWH_DB_PASSWORD')
    port     = config.get('DWH', 'DWH_PORT')
    conn     = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, dbname, user, password, port))
    cur      = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()