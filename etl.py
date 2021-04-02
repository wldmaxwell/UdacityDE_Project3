import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """ 
       Copies JSON data from S3 buckets into staging tables
        Arguments:
            * cur : creates connection to the Database to execute queries
            * conn : connects to database
        Returns:
            * Copies data into staging tables
    
        """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """ 
       Inserts data into dimension and fact tables from staging tables
        Arguments:
            * cur : creates connection to the Database to execute queries
            * conn : connects to database
        Returns:
            * Inserts data into dimension and fact tables
    
        """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """ 
        Connects to AWS, copies data into staging tables, inserts data into analytics tables, and closes connection
        Arguments:
            * cur : creates connection to the Database to execute queries
            * host : Redshift cluster address
            * dbname : dbname in reshift cluster
            * user : username for the db
            * password : password for the user
            * port : port number for redshift
            * conn : connects to database
            * config : get host, dbname, user, password, and port from dwg.cfg file
        Returns:
            * Copies JSON data into staging tables
            * Inserts data from staging tables into dimension and fact table
    
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