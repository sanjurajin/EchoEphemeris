import pandas as pd
import numpy as np
import requests
import zipfile
import os
import sys
from datetime import datetime
import time
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

cwd = os.getcwd()

# ---------------------------------------
# Read the creden file
# ---------------------------------------
# ---------------------------------------
if os.getlogin() == 'sanju':
    creden_file = os.path.join(cwd,'personal_data',"creden.txt")
    credentials = open(creden_file,'r').read().split()
    db_name = credentials[0]
    db_user = credentials[1]
    db_password = credentials[2]
else:
    creden_file = os.path.join(cwd,'app',"creden.txt")
    credentials = open(creden_file,'r').read().split()

    db_name = credentials[0]
    db_user = credentials[1]
    db_password = credentials[2]
# ---------------------------------------

# ---------------------------------------


setup_fl = os.path.join(cwd,'app','data', 'exch_files','initail_setup.csv')
combined_df_new  =pd.read_csv(setup_fl)


def get_old_static_data():
    db_config = { 'dbname': db_name,'user': db_user,
                'password': db_password, 'host': 'localhost',
                'port': '5432' }


    # Create the connection string 
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}" 
    # Create engine 
    engine = create_engine(conn_string) 
    # get all static data from database
    query = "SELECT isin FROM my_static_data"
    # query = "SELECT * FROM static_data"
    old_static = pd.read_sql_query(query, engine)
    existing_data = old_static[['isin']]
    # existing_isin_set = set(existing_data['isin'])
    # conn.close()
    # combined_df_new[['isin','src']]
    # print data types of combined_df_new
    # print(combined_df_new.dtypes)
    existing_isin_set  =set(existing_data['isin'])

     
    return existing_isin_set
# ----------------------------

def update_static_data(combined_df_new):
    static_data_clmns = ['isin','fininstrmid','fininstrmid_b','tckrsymb','tckrsymb_b','src','src_b','fininstrmnm']
    static_df = combined_df_new[static_data_clmns]
   
    invalid_fininstrmid_rows = static_df[
        (static_df['fininstrmid'].isnull() | (static_df['fininstrmid'] == 0) | (static_df['fininstrmid_b'] == '')) &
        (static_df['fininstrmid_b'].isnull() | (static_df['fininstrmid'] == 0)| (static_df['fininstrmid_b'] == ''))
            ]

    # Check conditions for 'src' and 'src_b'
    invalid_src_rows = static_df[
        (static_df['src'].isnull() | (static_df['src'] == 0) | (static_df['src_b'] == '')) &
        (static_df['src_b'].isnull() | (static_df['src'] == 0) | (static_df['src_b'] == ''))
            ]

    # Print messages for invalid 'fininstrmid' rows
    for index, row in invalid_fininstrmid_rows.iterrows():
        print(f"Invalid FININSTRMID at ISIN: {row['isin']} - Both columns are empty or invalid.")

    # Print messages for invalid 'src' rows
    for index, row in invalid_src_rows.iterrows():
        print(f"Invalid SRC at ISIN: {row['isin']} - Both columns are empty or invalid.")

    # Combine invalid rows into a single DataFrame if any
    invalid_rows = pd.concat([invalid_fininstrmid_rows, invalid_src_rows])

    # If there are invalid rows, process them
    if not invalid_rows.empty:
        # invalid_date = datetime.now().strftime('%d-%m-%Y')
        invalid_date = pd.to_datetime(combined_df_new['traddt'][0]).strftime('%d-%m-%Y')
        # Save to CSV file
        print(f"Invalid rows found. saving to CSV file (invalid_rows-{invalid_date}) and Exiting...")
        invalid_rows.to_csv(f'invalid_rows-{invalid_date}.csv', index=False)
        
        # Exit the program
        sys.exit()
    
    # ----------------------
    # Get old data
    existing_isin_set = get_old_static_data()
    
    # Assuming df_new_static is your new DataFrame
    missing_data = static_df[~static_df['isin'].isin(existing_isin_set)]

    # Updating static data
    if not missing_data.empty:
        print(f'----------Missing Data ------------')
        print(f'{missing_data}')
    
    db_config = { 'dbname': db_name,'user': db_user,
                'password': db_password, 'host': 'localhost',
                'port': '5432' }

    # Create the connection string 
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}" 
    # Create engine 
    engine = create_engine(conn_string) 

    if not missing_data.empty:
        missing_data.to_sql('my_static_data', engine, if_exists='append', index=False)
        print("New static data inserted in Databse for Static Data.")
    else:
        print("No new data to insert in Databse for Static Data.")


    # Return static_df
    # return static_df
    return missing_data

# -------------------------------------------

def update_daily_data(combined_df_new):
    # combined_df_new.columns
    
    columns_to_drop = ['fininstrmid', 'fininstrmid_b','tckrsymb', 'tckrsymb_b','src', 'src_b', 'fininstrmnm','Unnamed: 0']
    # drop columns from combined_df_new dataframe
    daily_data_df = combined_df_new.drop(columns=columns_to_drop)
    # daily_data_df.columns

    # Ensure the data types match the SQL table schema 
    daily_data_df['traddt'] = pd.to_datetime(daily_data_df['traddt'])

    # Ensure string columns do not exceed length limits 
    string_columns = { 'isin': 20, 'sctysrs': 10, 'sctysrs_b': 10, 'othr_trds': 20 }
    for col, length in string_columns.items():
        daily_data_df[col] = daily_data_df[col].astype(str).str.slice(0, length)


    integer_columns = [ 'ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b',
                        'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 
                        'othr_trds_vol', 'othr_trds_txsexctd']
    # Convert to integers, coercing errors to NaN 
    for col in integer_columns:
        daily_data_df[col] = pd.to_numeric(daily_data_df[col], errors='coerce').astype('Int64')    
    
    
    integer_columns = [ 'ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b',
                        'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 
                        'othr_trds_vol', 'othr_trds_txsexctd']
    # Convert to integers, coercing errors to NaN 
    for col in integer_columns:
        daily_data_df[col] = pd.to_numeric(daily_data_df[col], errors='coerce').astype('Int64')  


    decimal_columns = [ 'opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric',
    'prvsclsgpric', 'sttlmpric', 'del_per', 'avg_price', 'avg_order_price',
    'close_to_avg', 'close_avg_perc' ]
    
    # daily_data_df[decimal_columns]
    
    for col in decimal_columns:
        daily_data_df[col] = pd.to_numeric(daily_data_df[col], errors='coerce').astype(float)

    # daily_data_df['opnpric']
    

    # Define columns that may have decimal values
    decimal_columns = [ 'ttltrfval', 'delvry_trnovr', 'ttltrfval_b', 'delvry_trnovr_b',
                        'sum_ttltrfval', 'sum_delvry_trnovr', 'del_per', 'avg_price', 
                        'avg_qty_per_order', 'avg_order_price', 'close_to_avg',
                          'close_avg_perc', 'othr_trds_val' ]
    # Convert to floats, coercing errors to NaN 
    for col in decimal_columns:
        daily_data_df[col] = pd.to_numeric(daily_data_df[col], errors='coerce').astype(float)

    
        # 5 update dataily data table with this new dataframe
    # db_config = { 'dbname': 'delivery_data','user': 'postgres',
    #             'password': 'aurvaprah', 'host': 'localhost',
    #             'port': '5432' }
    db_config = { 'dbname': db_name,'user': db_user,
                'password': db_password, 'host': 'localhost',
                'port': '5432' }
    # Create the connection string 
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}" 
    # Create engine 
    engine = create_engine(conn_string) 
    try:
        # Upload DataFrame to PostgreSQL 
        daily_data_df.to_sql('my_daily_data', engine, if_exists='append', index=False)
        print("Daily Data uploaded successfully.")
    except IntegrityError as e:
        print(f"Error occurred: {e.orig}")
     # Handle specific integrity error, e.g., duplicate key 
    except Exception as e:
        print(f"An error occurred: {e}") # Handle other potential errors
    finally:
        # Commit changes and close connection 
        try: engine.dispose()
     # Ensures that all connections are closed print("Database connection closed.")
        except Exception as e:
            print(f"Error closing the connection: {e}")

    return daily_data_df



# ---------------------------------------

def initial_setup(combined_df_new):
    update_static_data(combined_df_new)
    update_daily_data(combined_df_new)


initial_setup(combined_df_new)


