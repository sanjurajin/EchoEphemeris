import psycopg2

def create_database_and_table():
    # Connect to the default database to create a new one
    conn = psycopg2.connect(
        database="postgres",  # Connect to the default database
        user="your_username",  # Replace with your PostgreSQL username
        password="your_password",  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )
    
    conn.autocommit = True  # Enable autocommit mode
    cursor = conn.cursor()
    
    # Create database
    cursor.execute("CREATE DATABASE my_delivery_data;")
    print("Database 'my_delivery_data' created successfully.")
    
    # Close the connection to execute commands on the new database
    cursor.close()
    conn.close()

    # Connect to the new database to create a table
    conn = psycopg2.connect(
        database="my_delivery_data",
        user="your_username",  # Replace with your PostgreSQL username
        password="your_password",  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )
    
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("""
        CREATE TABLE my_static_data (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    print("Table 'my_static_data' created successfully.")
    
    # Clean up and close connections
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_database_and_table()
