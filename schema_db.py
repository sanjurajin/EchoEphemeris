import os
import psycopg2


cwd = os.getcwd()

# ---------------------------------------
# Read the creden file
creden_file = os.path.join(cwd,'app',"creden.txt")
credentials = open(creden_file,'r').read().split()

db_name = credentials[0]
db_user = credentials[1]
db_password = credentials[2]
# ---------------------------------------

db_name = "echoephemeris"
db_password = 'aurvaprah'

def create_database(db_password):
    # Connect to the default database to create a new one
    conn = psycopg2.connect(
        database="postgres",  # Connect to the default database
        user="postgres",  # Replace with your PostgreSQL username, default used
        password=db_password,  # Replace with your PostgreSQL password, from creden.txt
        host="localhost",
        port="5432"
    )
    
    conn.autocommit = True  # Enable autocommit mode
    cursor = conn.cursor()
    try:    
        # Create database
        cursor.execute("CREATE DATABASE EchoEphemeris;")
        print("Database 'my_delivery_data' created successfully.")
        status = True
    except Exception as e:
        print(f"An error occurred: {e}") # Handle other potential errors
        status = False
    finally:
        # Close the connection to execute commands on the new database
        cursor.close()
        conn.close()
    return status

# --------------------------
def static_tb(db_name, db_password):
    # Connect to the new database to create a table
    conn = psycopg2.connect(
        database=db_name,
        user="postgres",  # Replace with your PostgreSQL username
        password=db_password,  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()
    try:
    # Create table
        cursor.execute("""
                      CREATE TABLE my_static_data (
                            static_id SERIAL PRIMARY KEY,
                            isin VARCHAR(20) NOT NULL UNIQUE,
                            fininstrmid VARCHAR(10),
                            fininstrmid_b VARCHAR(10),
                            tckrsymb VARCHAR(20),
                            tckrsymb_b VARCHAR(20),
                            src VARCHAR(10),
                            src_b VARCHAR(10),
                            fininstrmnm VARCHAR(50) );

                    """)

        print("Table 'my_static_data' created successfully.")
        status = True
    except Exception as e:
        print(f"An error occurred: {e}") # Handle other potential errors
        status = False
    finally:
        # Close the connection to execute commands on the new database
        cursor.close()
        conn.commit()
        conn.close()
    return status


# --------------------------
def daily_tb(db_name, db_password):
    # Connect to the new database to create a table
    conn = psycopg2.connect(
        database=db_name,
        user="postgres",  # Replace with your PostgreSQL username
        password=db_password,  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()
    try:
    # Create table
        cursor.execute("""
                       CREATE TABLE my_daily_data (
                            ISIN VARCHAR(20) NOT NULL,
                            TradDt DATE NOT NULL,
                            sctysrs VARCHAR(10),
                            sctysrs_b VARCHAR(10),
                            opnpric DECIMAL(12,2),
                            hghpric DECIMAL(12,2),
                            lwpric DECIMAL(12,2),
                            clspric DECIMAL(12,2),
                            lastpric DECIMAL(12,2),
                            prvsclsgpric DECIMAL(12,2),
                            sttlmpric DECIMAL(12,2),
                            ttltradgvol BIGINT,
                            ttltrfval BIGINT,
                            ttlnboftxsexctd BIGINT,
                            qty_del BIGINT,
                            delvry_trnovr BIGINT,
                            ttltradgvol_b BIGINT,
                            ttltrfval_b BIGINT,
                            ttlnboftxsexctd_b BIGINT,
                            qty_del_b BIGINT,
                            delvry_trnovr_b BIGINT,
                            sum_ttltradgvol BIGINT,
                            sum_ttltrfval BIGINT,
                            sum_ttlnboftxsexctd BIGINT,
                            sum_del_qty BIGINT,
                            sum_delvry_trnovr BIGINT,
                            del_per DECIMAL(12,2),
                            avg_price DECIMAL(12,2),
                            avg_qty_per_order BIGINT,
                            avg_order_price DECIMAL(12,2),
                            close_to_avg DECIMAL(12,2),
                            close_avg_perc DECIMAL(12,2),
                            othr_trds VARCHAR(20),
                            othr_trds_vol BIGINT,
                            othr_trds_val BIGINT,
                            othr_trds_txsexctd BIGINT,
                            events TEXT,
                            FOREIGN KEY (ISIN) REFERENCES my_static_data(ISIN) ON DELETE CASCADE,
                            CONSTRAINT my_daily_data_pkey PRIMARY KEY (ISIN, TradDt, sctysrs, sctysrs_b)
                                )
                                PARTITION BY RANGE (TradDt);
                        """)

        print("Table 'my_daily_data' created successfully.")
        status = True
    except Exception as e:
        print(f"An error occurred: {e}") # Handle other potential errors
        status = False
    finally:
        # Close the connection to execute commands on the new database
        cursor.close()
        conn.commit()
        conn.close()
    return status






# --------------------------
def daily_tb_part(db_name, db_password):
    # Connect to the new database to create a table
    conn = psycopg2.connect(
        database=db_name,
        user="postgres",  # Replace with your PostgreSQL username
        password=db_password,  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()
    try:
    # Create table
        cursor.execute("""
                        CREATE TABLE my_daily_data_q1_2024 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
                        CREATE TABLE my_daily_data_q2_2024 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
                        CREATE TABLE my_daily_data_q3_2024 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
                        CREATE TABLE my_daily_data_q4_2024 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
                        CREATE TABLE my_daily_data_q1_2025 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
                        CREATE TABLE my_daily_data_q2_2025 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
                        CREATE TABLE my_daily_data_q3_2025 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');
                        CREATE TABLE my_daily_data_q4_2025 PARTITION OF my_daily_data
                            FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');
                       
                        """)

        print("Partitions for 'my_daily_data' created successfully.")
        status = True
    except Exception as e:
        print(f"An error occurred: {e}") # Handle other potential errors
        status = False
    finally:
        # Close the connection to execute commands on the new database
        cursor.close()
        conn.commit()
        conn.close()
    return status



# --------------------------
def mapping_tb_(db_name, db_password):
    # Connect to the new database to create a table
    conn = psycopg2.connect(
        database=db_name,
        user="postgres",  # Replace with your PostgreSQL username
        password=db_password,  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()
    try:
    # Create table
        cursor.execute("""
                        CREATE TABLE isin_mapping (
                            mapping_id SERIAL PRIMARY KEY,  -- Unique identifier for each mapping
                            symbol VARCHAR(20),
                            isin character varying(12) NOT NULL,
                            ref_isin character varying(12) NOT NULL,
                            FOREIGN KEY (isin) REFERENCES my_static_data(isin),  -- Ensure isin exists in static_data
                            FOREIGN KEY (ref_isin) REFERENCES my_static_data(isin),  -- Ensure ref_isin exists in static_data
                            UNIQUE (isin, ref_isin)  -- Ensure that the combination of isin and ref_isin is unique
                        );
                        """)

        print("Table  'isin_mapping' created successfully.")
        status = True
    except Exception as e:
        print(f"An error occurred: {e}") # Handle other potential errors
        status = False
    finally:
        # Close the connection to execute commands on the new database
        cursor.close()
        conn.commit()
        conn.close()
    return status



def list_tables(db_name, db_user, db_password):
    # Database connection parameters
    conn = psycopg2.connect(
        database=db_name,  # Replace with your database name
        user=db_user,          # Replace with your PostgreSQL username
        password=db_password,      # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )

    # Create a cursor object
    cursor = conn.cursor()
    try:
            
        # Execute the query to get all table names in the current database
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';  -- Change schema if needed
        """)

        # Fetch all results and print each table name
        tables = cursor.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(table[0])  # Print the table name
    except Exception as e:
        print(f"An error occurred: {e} while listing Tables") # Handle other potential errors
        status = False
    finally:
        # Close the connection to execute commands on the new database
        cursor.close()
        conn.close()
    



def check_database_exists(db_name, db_password):
    from psycopg2 import sql
    # Connect to the default database to perform the check
    conn = psycopg2.connect(
        database="postgres",  # Connect to the default database
        user="postgres",  # Replace with your PostgreSQL username
        password=db_password,  # Replace with your PostgreSQL password
        host="localhost",
        port="5432"
    )
    
    cursor = conn.cursor()
    try:
        # Query to check if the database exists
        query = sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;")
        
        cursor.execute(query, (db_name,))
        exists = cursor.fetchone() is not None  # Check if any row was returned
    except Exception as e:
        print(f"An error occurred: {e}") # Handle other potential errors
        status = False
    finally:
        # Close the connection to execute commands on the new database
        cursor.close()
        
        conn.close()    
    if exists:
        print(f"The database '{db_name}' already exists.")
        status = False
    else:
        print(f"The database '{db_name}' does not exist.")
        status = True
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    return status

status = check_database_exists(db_name, db_password)
if status:
    status = create_database(db_password=db_password)
if status:
    status = static_tb(db_name=db_name, db_password=db_password)
if status:
    status = daily_tb(db_name=db_name, db_password=db_password)
if status:    
    status  = daily_tb_part(db_name=db_name, db_password=db_password)
if status:
    status  =  mapping_tb_(db_name=db_name, db_password=db_password)
if status:
    print('Database setup successfully done')
    list_tables(db_name, db_user, db_password) 
if not status:
    list_tables(db_name, db_user, db_password)