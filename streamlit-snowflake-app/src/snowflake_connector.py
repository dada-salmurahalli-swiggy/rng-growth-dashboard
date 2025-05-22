def get_connection():
    import snowflake.connector # type: ignore

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user='YOUR_USERNAME',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT',
        warehouse='YOUR_WAREHOUSE',
        database='YOUR_DATABASE',
        schema='YOUR_SCHEMA'
    )
    return conn

def fetch_tables(conn):
    cursor = conn.cursor()
    try:
        # Execute a query to fetch the list of tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return tables
    finally:
        cursor.close()