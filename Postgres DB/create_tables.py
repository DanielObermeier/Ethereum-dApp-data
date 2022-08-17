import psycopg2
from sql_queries import create_table_queries, drop_table_queries
#from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the postgres database
    - Returns the connection and cursor to hte postgres database
    """
    
    # connect to default database
    conn = psycopg2.connect(
            host="localhost",
            dbname="postgres",
            user="postgres",
            password="") #insert password

    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS postgres")
    cur.execute("CREATE DATABASE postgres WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    connect = "host=localhost dbname=postgres user=postgres password=" 
    password = "" # load password from environment
    conn = psycopg2.connect(connect+password)
    cur = conn.cursor()
    
    return cur, conn


def connect_database():

    """
    - Connects to the existing postgres database, this DB is created in SQL commandline and can be viewd under pg admin

    """
    conn = psycopg2.connect(
        host="localhost",
        dbname="postgres",
        user="postgres",
        password="") # load password from environment

    conn.set_session(autocommit=True)
    cur = conn.cursor()

    print("has successfully connceted to db")
    

    return cur, conn



def drop_connections(cur, conn): 
    """
    Drops all other connections to the db in order to be able to drop tables
    """

    drop_command = """SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = 'postgres' 
                        AND pid <> pg_backend_pid();"""


    cur.execute(drop_command)
    conn.commit()

    print("has successfully closed all connections")




def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    print("has successfully dropped all tables")




def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
    print("has successfully created all tables")



def main():
    """
    - Establishes connection with the postgres database and gets
    cursor to it.  

    - Disconnects all other connections 
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = connect_database()
    
    # drops connections
    drop_connections(cur, conn)
    # drops tables
    drop_tables(cur, conn)
    #creates tables
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()