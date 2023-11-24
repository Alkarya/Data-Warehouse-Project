import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description:
        runs the statements needed to copy the data from S3 to redshift
    Arguments:
        cur - cursor object to execute aws redshift command
        conn - connection object to db
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description:
        Loads the data from staging tables to fact and dimension tables
    Arguments:
        cur - cursor object to execute aws redshift command
        conn - connection object to db
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description:
        Loads and reads dwh.cfg config file
        Connects to Redshift using the configs extracted from the file
        Runs the load_staging_tables and insert_tables methods
        Closes the connection
    Arguments:
        cur - cursor object to execute aws redshift command
        conn - connection object to db
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()