import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
     """ 
        Drops Existing Tables from AWS Redshift cluster
        Arguments:
            * cur : creates connection to the Database to execute queries
            * conn : connects to database
        Returns:
            * Tables Dropped
    
        """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
         """ 
        Creates tables if they don't exist in AWS Redshift cluster
        Arguments:
            * cur : creates connection to the Database to execute queries
            * conn : connects to database
        Returns:
            * Tables Created
    
        """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """ 
        Connects to AWS, drops existing tables, creates new tables, and closes connection
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
            * Created tables in AWS Redshift cluster
    
        """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()