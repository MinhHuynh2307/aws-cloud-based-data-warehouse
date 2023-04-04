import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Load data from S3 to staging tables on Redshift
    
    Args:
        cur  : a cursor
        conn : connection to the database
    
    Returns:
        Results from sql query execution
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from staging tables to analytics tables (fact and dimension tables) on Redshift.
    
    Args:
        cur  : a cursor
        conn : connection to the database
    
    Returns:
        Results from sql query execution
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to Redshift database, get a cursor, then run load_staging_tables() and insert_tables() functions
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()