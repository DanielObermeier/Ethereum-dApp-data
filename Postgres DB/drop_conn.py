import psycopg2


def connect_database():

    """
    - Connects to the existing dapps_data database, this DB is created in SQL commandline and can be viewd under pg admin

    """
    conn = psycopg2.connect(
        host="localhost",
        dbname="dapps_data",
        user="postgres",
        password="") #load password from environment
        
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
                        WHERE pg_stat_activity.datname = 'dapps_data' 
                        AND pid <> pg_backend_pid();"""


    cur.execute(drop_command)
    conn.commit()

    print("has successfully closed all connections")


def main():
    """
    - Establishes connection with the dapps_data database and gets
    cursor to it.  
    
    - Drops all connections to the DB
    - Finally, closes the connection. 
    """
    cur, conn = connect_database()
    
    drop_connections(cur, conn)

    conn.close()

    print("has successsfully closed own connections")

if __name__ == "__main__":
    main()