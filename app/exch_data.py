

import pandas as pd
import requests
import zipfile
import os
from datetime import datetime
import time
from sqlalchemy import create_engine
cwd = os.getcwd()

import psycopg2
from sqlalchemy.exc import SQLAlchemyError
# import logging


# ---------------------------------------
# Read the creden file
creden_file = os.path.join(cwd,'app',"creden.txt")
credentials = open(creden_file,'r').read().split()

db_name = credentials[0]
db_user = credentials[1]
db_password = credentials[2]

# ---------------------------------------
db_name = 'echoephemeris'
db_password = 'aurvaprah'
# ---------------------------------------
# -------------------------------------
#  GET SYMBOLS from DATABSE
 

def get_postgresdb_symbols():
    db_config = { 'dbname': db_name,'user': db_user,
                'password': db_password, 'host': 'localhost',
                'port': '5432' }




    # Create the connection string
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

    # Create engine
    engine = create_engine(conn_string)

    # Define the query
    query = '''
    SELECT *
    FROM my_static_data
    '''
    # Read SQL query into a DataFrame using SQLAlchemy engine
    try:
        symbol_data_df = pd.read_sql(query, engine)
       
        # print(daily_data_df)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        engine.dispose()
        print("Database connection closed.")
    
    symbol_data_df.columns
    
    symbol_data_df['tckrsymb'] =symbol_data_df['tckrsymb'].fillna(symbol_data_df['tckrsymb_b'])

    # symbols_df = symbol_data_df[['tckrsymb', 'fininstrmnm', 'isin']]
    
    symbol_data_df = symbol_data_df.sort_values(by='fininstrmnm')
    symbol_data_df = symbol_data_df.reset_index()
    symbols_df = symbol_data_df[['tckrsymb','fininstrmnm','isin']]
    symbols_df_n = pd.DataFrame()
    symbols_df_n['symbol_name'] = symbols_df['tckrsymb'] + " :- " + symbols_df['fininstrmnm'] + "  "+ symbols_df['isin']
    symbols_list = symbols_df_n['symbol_name'].tolist()


    return symbols_list


# -------------------------

# GET POSTGRESQL DATA

def get_postgresdb_data(symbol):
    try:
        # logger.info(f"Processing symbol: {symbol}")
        # symbol = ' AARTECH :- AARTECH SOLONICS LIMITED  INE01C001026'
        # Split the string by ':-' and then strip whitespace
        parts = symbol.split(':-')
        
        # Extract the script code, script name, and ISIN value
        script_code = parts[0].strip()
        script_name = parts[1].strip().rsplit(' ', 1)[0]
        isin_value = parts[1].strip().rsplit(' ', 1)[1]  
        
        # logger.info(f"ISIN Value: {isin_value}")

     
        db_config = { 'dbname': db_name,'user': db_user,
                'password': db_password, 'host': 'localhost',
                'port': '5432' }



        # Create the connection string
        conn_string = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )

        # Create engine with timeout
        engine = create_engine(
            conn_string,
            connect_args={'connect_timeout': 10}  # 10 seconds timeout
        )

        # Correct SQL query with proper parameter binding
        query = """
            SELECT * FROM my_daily_data
            WHERE isin = %(isin)s
        """

        # logger.info("Executing database query...")
        
        # Execute query with timeout
        with engine.connect().execution_options(timeout=30) as connection:  # 30 seconds timeout
            daily_data_df = pd.read_sql(
                query,
                connection,
                params={'isin': isin_value}
            )

        # logger.info(f"Query returned {len(daily_data_df)} rows")
        
        if daily_data_df.empty:
            # logger.warning(f"No data found for ISIN: {isin_value}")
            print(f"No data found for ISIN: {isin_value}")
            return pd.DataFrame(), "None"  # Return empty DataFrame instead of None

        # Display first 10 rows
        # print(daily_data_df[['isin','traddt','clspric','del_per']].head(10))

        # Make other data
            
        # isin_daily_df = isin_daily_df[[ 'traddt','opnpric', 'hghpric', 'lwpric',
        #             'clspric', 'sum_delvry_trnovr','avg_order_price','sum_ttltrfval','del_per']].copy()
        daily_data_df['traddt'] = pd.to_datetime(daily_data_df['traddt'], dayfirst=True)
        daily_data_df['time'] = daily_data_df['traddt']
        daily_data_df = daily_data_df.sort_values(by='time')
        daily_data_df = daily_data_df.drop(columns=['traddt'])

        deli_val_sum = daily_data_df['sum_delvry_trnovr'].sum()
        turnover_sum = daily_data_df['sum_ttltrfval'].sum()
        avg_del_perc = deli_val_sum*100/turnover_sum

        daily_data_df['avg_del_perc'] = avg_del_perc.round(4)
        avg_order_worth = daily_data_df['avg_order_price'].mean().round(2)
        daily_data_df['avg_order_price'] = (daily_data_df['avg_order_price']/1000).round(3)
        # isin_daily_df['avg_of_etw'] = (avg_order_worth/1000)
        daily_data_df['avg_of_aop'] = (avg_order_worth)
        daily_data_df['avg_of_aop'] = (daily_data_df['avg_of_aop']/1000).round(3)
        # isin_daily_df['avg_order_price'] = (isin_daily_df['avg_order_price']/10).round(2)
        daily_data_df['sum_delvry_trnovr'] = (daily_data_df['sum_delvry_trnovr']/1000000).round(3)
        daily_data_df.rename(columns={'opnpric': 'open', 'hghpric': 'high','lwpric': 'low','clspric': 'close'}, inplace=True)
        # isin_daily_df = isin_daily_df.reindex()
        daily_data_df = daily_data_df.reindex(columns=['time', 'open', 'high', 'low','close', 'sum_delvry_trnovr', 'del_per','avg_order_price','avg_del_perc', 'sum_del_qty','avg_of_aop','events' ])
        # isin_daily_df.to_csv('isin_irctc.csv')
        
        return daily_data_df, script_name
    
    except SQLAlchemyError as e:
        # logger.error(f"Database error occurred: {str(e)}")
        print(f"Database error occurred: {str(e)}")
        return pd.DataFrame(), "None"
        
    except Exception as e:
        # logger.error(f"An unexpected error occurred: {str(e)}")
        print(f"An unexpected error occurred: {str(e)}")
        return pd.DataFrame(), "None"
        
    finally:
        try:
            engine.dispose()
            # logger.info("Database connection closed.")
            print("Database connection closed.")
        except:
            pass

# ---------------------------
# ---------------------------
# Download NSE BSE Files
# Define the directory where you want to save the files
download_directory = os.path.join(cwd,'app','data', 'exch_files') #'data/exch_files'

# ---------------------------------------
# FIRST BLOCK
def last_business_day():
    calander_file_path = os.path.join(cwd,'app','data', 'exch_files','calander_file.csv') #'data/exch_files'
    date_file = pd.read_csv(calander_file_path) #'Voltas_2022_2024.csv')
    last_day = (date_file[-1:].values)[0]
    last_day = pd.to_datetime(last_day[0], format='%d-%b-%y').strftime('%d-%m-%Y')
    return last_day


def updates_dates(new_dates):
    # print('printng from exch_data py')
    # print(new_dates)
    # new_dates = ['25-11-2024', '26-11-2024', '01-12-2024', '02-12-2024']
    new_dates_df= pd.DataFrame(new_dates, columns=['Date'])
    new_dates_df['Date'] =(pd.to_datetime(new_dates_df['Date'], format='%d-%m-%Y', dayfirst=True)).dt.strftime('%d-%b-%y')
    calander_file_path = os.path.join(cwd,'app','data', 'exch_files','calander_file.csv') #'data/exch_files'
    # date_file = pd.read_csv(calander_file_path) 
    # df = pd.read_csv(date_file)
    new_dates_df.to_csv(calander_file_path, mode='a', header=False, index=False)

    status = 'Date Data File Updated'
       
    return status

# ---------------------------------------
# SECOND BLOCK

def download_nse_bse_files_1(start_date, end_date):
    # print('from exch_data.py file')
    print(start_date, end_date)
    # print(type(start_date), type(end_date))
    # convert start_date, end_date to datetime from text format '2024-11-05'
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    if start_date == end_date or start_date < end_date:
        # print('get todays data')
        # CODES TO BE ADDED HERE
        
    # elif start_date < end_date:
        data_month = start_date.month
        data_year = start_date.year
        dt_list = get_date_range(data_month, data_year)
        dt_list['Date_type'] = pd.to_datetime(dt_list['Date_type'])
        # delete all the rows where Date is not between start_date and end_date
        dt_list = dt_list[(dt_list['Date_type'] >= start_date) & (dt_list['Date_type'] <= end_date)]
        if not dt_list.empty:
                
            for index, row in dt_list.iterrows():
                d_date = row['Date_type']  # Get the date as a string
                bhavcopy_date = row['bhavcopy_Date']  # Get the date as a string
                delivery_date = row['delivery_Date']  # Get the date as a string
                old_date = row['old_date']
                year_val= row['year']
                dtmonth = row['dtmonth']
                # if d_date.day is 1 to 8 print date
                # if d_date.day in range(6, 8):
                print('--------------------------')
                print(row['Date'])

                try:
                    download_nse_files_1(bhavcopy_date, delivery_date)
                except Exception as e:
                    print(f"Error downloading NSE files for {bhavcopy_date}: {e}")
                try:
                    download_bse_files_1(bhavcopy_date, old_date, year_val, dtmonth)
                except Exception as e:
                    print(f"Error downloading NSE files for {bhavcopy_date}: {e}")

                time.sleep(10)
            status = 'Data Downloaded Successfully'
        else:
            print('No Dates found between start_date and end_date in Calender File')
            status = 'No Dates found between start_date and end_date in Calender File'
           
    else:
        print('start_date is greater than end_date')
        status = 'start_date is greater than end_date'


    return status


 # -------------------------
# ---------------------------------------
def get_date_range(data_month, data_year):
    # Updated file with all columns
    calander_file_path = os.path.join(cwd,'app','data', 'exch_files','calander_file.csv') #'data/exch_files'
    date_file = pd.read_csv(calander_file_path) 
    
    date_file['bhavcopy_Date'] = pd.to_datetime(date_file['Date'], format='%d-%b-%y').dt.strftime('%Y%m%d')
    # Convert the string to a datetime object
    date_file['delivery_Date'] =  pd.to_datetime(date_file['Date'], format='%d-%b-%y').dt.strftime('%d%m%Y')
    date_file['Date_type'] = pd.to_datetime(date_file['Date'], format='%d-%b-%y')
    
    date_file['old_date'] = pd.to_datetime(date_file['Date'], format='%d-%b-%y').dt.strftime('%d%m%y')
    date_file['year'] = pd.to_datetime(date_file['Date'], format='%d-%b-%y').dt.year
    date_file['date'] = pd.to_datetime(date_file['Date'], format='%d-%b-%y').dt.day
    date_file['month'] =  pd.to_datetime(date_file['Date'], format='%d-%b-%y').dt.month
    
    # Create a new 'datemonth' column by concatenating 'date' and 'month'
    date_file['dtmonth'] = date_file['date'].astype(str).str.zfill(2) + date_file['month'].astype(str).str.zfill(2)
    # list_dt = date_file[(date_file['Date_type'].dt.month == month) & (date_file['Date_type'].dt.year == year)]['bhavcopy_Date']
    list_dt = date_file[(date_file['Date_type'].dt.month == data_month) & (date_file['Date_type'].dt.year == data_year)]
    return list_dt
    

def download_nse_files_1(bhavcopy_date, delivery_date):
        # bhavcopy_nse =  'https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20241108_F_0000.csv.zip'
        bhavcopy_nse =  f'https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{bhavcopy_date}_F_0000.csv.zip'
        delivery_nse = f'https://nsearchives.nseindia.com/archives/equities/mto/MTO_{delivery_date}.DAT'

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
        response = requests.get(bhavcopy_nse, headers=headers)
        # Check if the request was successful

        
        zip_file_path = os.path.join(download_directory)
        zip_file =  os.path.join(download_directory,f'BhavCopy_NSE_CM_0_0_0_{bhavcopy_date}_F_0000.csv.zip')

        if response.status_code == 200:
            # Open a file in binary write mode
            with open(zip_file, 'wb') as file:
                file.write(response.content)  # Write the content to the file
            # print("Download completed successfully!")
            
            # Unzipping the downloaded file and exreact to download_directory
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(os.path.join(download_directory,"NSE Files"))  

            print("Bhavcopy_NSE completed successfully!")
            os.remove(zip_file)
            bhavcopy_done = True  # Mark that an action was taken
            
        else:
            # bhavdata_date= date.strftime('%d%m%Y')
            payload=pd.read_csv("https://archives.nseindia.com/products/content/sec_bhavdata_full_"+delivery_date+".csv")
            # if payload is not empty
            if not payload.empty:
                # payload.columns
                payload.columns = payload.columns.str.strip()
                payload['turnover'] = payload['AVG_PRICE']*payload['TTL_TRD_QNTY']
                payload.drop(columns=['TURNOVER_LACS'],inplace=True)
                # payload['TURNOVER_LACS'].astype(int)

                file_name = os.path.join(download_directory,"NSE Files",f'BhavCopy_NSE_CM_0_0_0_{bhavcopy_date}_F_0000_O.csv')
                payload.to_csv(file_name,index=False)
                print("Bhavcopy_NSE Old completed successfully!")
                bhavcopy_done = True
            else:
                print(f"Failed to download NSE file.")
        if bhavcopy_done:
            response = requests.get(delivery_nse, headers=headers)    
            if response.status_code == 200:
                # Open a file in binary write mode
                file_name = os.path.join(download_directory,'NSE Files', f'MTO_{delivery_date}.DAT')
                with open(file_name, 'wb') as file:
                    file.write(response.content)  # Write the content to the file
                print("Delivery NSE completed successfully!")
            else:
                print(f"Failed to download Delivery NSE file.")
        else:
            print(f"Failed to download NSE Bhavcopy file, skipping Delivery.")


def download_bse_files_1(bhavcopy_date, old_date, year_val, dtmonth):    
    
        bhavcopy_bse_new = f"https://www.bseindia.com//download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{bhavcopy_date}_F_0000.CSV"

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
        response = requests.get(bhavcopy_bse_new, headers=headers)

        zip_file_path = os.path.join(download_directory)
        file =  os.path.join(download_directory,'BSE Files',f'BhavCopy_BSE_CM_0_0_0_{bhavcopy_date}_F_0000.csv')

        if response.status_code == 200:
            # Open a file in binary write mode
            with open(file, 'wb') as file:
                file.write(response.content)  # Write the content to the file
            print("Bhavcopy_BSE completed successfully!")
        else:
            # print(f"Failed to download New file. Status code: {response.status_code}")
            
            bhavcopy_bse_old = f'https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{old_date}.zip'
            response = requests.get(bhavcopy_bse_old, headers=headers)
            if response.status_code == 200:
            # Open a file in binary write mode
                file =  os.path.join(download_directory,'BSE Files',f'BhavCopy_BSE_CM_0_0_0_{bhavcopy_date}_F_0000_O.csv')
                with open(file, 'wb') as file:
                    file.write(response.content)  # Write the content to the file
                print("Bhavcopy_BSE Old completed successfully!")
            else:
                print(f"Failed to download BSE Bhavcopy file")
                
        delivery_bse = f'https://www.bseindia.com/BSEDATA/gross/{year_val}/SCBSEALL{dtmonth}.zip'

        response = requests.get(delivery_bse, headers=headers)
        if response.status_code == 200:
            # Open a file in binary write mode
            del_file =os.path.join(download_directory,'BSE Files',f'SCBSEALL{bhavcopy_date}.zip')
            bse_dir = os.path.join(download_directory,'BSE Files') 
            with open(del_file, 'wb') as file:
                file.write(response.content)  # Write the content to the file
            
            # Unzipping the downloaded file
            with zipfile.ZipFile(del_file, 'r') as zip_ref:
                zip_ref.extractall(bse_dir)  # Extracts to the specified directory
                file_list = zip_ref.namelist()  # List the contents of the ZIP file
            if file_list:
                old_file_name = file_list[0]  # Get the name of the first (and only) file
                new_file_name = f'SCBSEALL{bhavcopy_date}.txt'
                
                # Define the old and new file paths
                old_file_path = os.path.join(bse_dir, old_file_name)
                new_file_path = os.path.join(bse_dir, new_file_name)

                # Rename the extracted file
                os.rename(old_file_path, new_file_path)
                remove_file = os.path.join(bse_dir,del_file)
                os.remove(remove_file)
                print("Delivery BSE completed successfully!")

            else:
                print(f"Failed to download Delivery BSE file")
        else:
            print(f"Failed to download Delivery BSE file")
