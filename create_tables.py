import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description:
        Drops all tables
    Arguments:
        cur - cursor object to execute aws redshift command
        conn - connection object to db
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description:
        Creates tables used in redshift
    Arguments:
        cur - cursor object to execute aws redshift command
        conn - connection object to db
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Description:
        Loads and reads dwh.cfg config file
        Connects to Redshift using the configs extracted from the file
        Runs the drop_tables and create_tables methods
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
    print(conn)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()