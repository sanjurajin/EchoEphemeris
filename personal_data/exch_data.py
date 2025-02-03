

import pandas as pd
import requests
import zipfile
import sys
import os
from datetime import datetime
import time
from sqlalchemy import create_engine
cwd = os.getcwd()
import numpy as np
import psycopg2
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import IntegrityError
# import logging


# ---------------------------------------
# Read the creden file

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


# ---------------------------------------
# THIRD BLOCK


def get_bhavcopy_path(data_day, data_month, data_year):
    
    if type(data_day) == int:
        data_day = f"{data_day:02d}"
    if type(data_month) == int:
        data_month = f"{data_month:02d}"
    nse_file_path = os.path.join(cwd,'app','data','exch_files','NSE Files')
    bse_file_path = os.path.join(cwd,'app','data','exch_files','BSE Files')
    file_nd_dat = os.path.join(nse_file_path, f'MTO_{data_day}{data_month}{data_year}.DAT')  
    # file_bd_txt = os.path.join(cwd,'BSE Files', f'SCBSEALL{data_year}{data_month}{data_day}.txt')
    file_bd_txt = os.path.join(bse_file_path, f'SCBSEALL{data_year}{data_month}{data_day}.txt')
    
    # Define the file paths
    first_file_nse = os.path.join(nse_file_path, f'BhavCopy_NSE_CM_0_0_0_{data_year}{data_month}{data_day}_F_0000.csv')  
    second_file_nse = os.path.join(nse_file_path, f'BhavCopy_NSE_CM_0_0_0_{data_year}{data_month}{data_day}_F_0000_O.csv')  
    first_file_bse = os.path.join(bse_file_path, f'BhavCopy_BSE_CM_0_0_0_{data_year}{data_month}{data_day}_F_0000.csv')  
    # second_file_bse = os.path.join(cwd,'BSE Files', f'BhavCopy_BSE_CM_0_0_0_{data_year}{data_month}{data_day}_F_0000_O.csv')  
    second_file_bse = os.path.join(bse_file_path, f'BhavCopy_BSE_CM_0_0_0_{data_year}{data_month}{data_day}_F_0000_O.csv')  


    if os.path.isfile(first_file_nse):
        file_nb_csv = first_file_nse
    else:
        if os.path.isfile(second_file_nse):
            file_nb_csv = second_file_nse
        else:
            print("Neither New nor Old NSE Bhavcopy file is available.")
            file_nb_csv = ''
    
    if os.path.isfile(first_file_bse):
        file_bb_csv = first_file_bse
    else:
        if os.path.isfile(second_file_bse):
            file_bb_csv = second_file_bse
        else:
            print("Neither New nor Old BSE Bhavcopy file is available.")
            file_bb_csv = ''
    return file_nd_dat, file_bd_txt, file_nb_csv, file_bb_csv


def check_nse_dates(df_ncsv,file_nd_dat ):
    if 'TradDt' in df_ncsv.columns:
        ncsv_date = df_ncsv['TradDt'][0]
    elif 'DATE1' in df_ncsv.columns:
        ncsv_date = df_ncsv['DATE1'][0].strip()
        ncsv_date = datetime.strptime(ncsv_date, '%d-%b-%Y').strftime('%Y-%m-%d')
    else:
        ncsv_date = None  # or handle the case when neither column exists
     
    with open(file_nd_dat, 'r') as file:
        lines = file.readlines()
        # Check if there are at least three lines
        if len(lines) > 1:
            # Split the second line and get the third value
            second_line = lines[1].strip()  # Get the second line and remove any trailing spaces/newlines
            values = second_line.split(',')  # Split the line by comma
            if len(values) > 2:  # Ensure there are enough values
                settlement_date = values[2]  # Get the third value
        ncsv_del_date = settlement_date
        ncsv_del_date = datetime.strptime(ncsv_del_date, '%d%m%Y').strftime('%Y-%m-%d')

        
    # Now compare the two dates
    if ncsv_del_date == ncsv_date:
        # print("The dates are the same for NSE Files.")
        flag= 'True'
    else:
        # print("The dates are different for NSE Files.")
        flag= 'Flase'
    return flag

def read_nse_files(file_nd_dat,file_nb_csv):

    data = []

    # Read the .dat file and parse it
    with open(file_nd_dat, 'r') as file:
        # Skip the first three lines that do not contain data
        for _ in range(3):
            next(file)
            
        # Process the lines containing the records
        for line in file:
            # Strip whitespace and check for empty lines
            line = line.strip()
            
            if line:
                # Split the line by commas
                parts = line.split(',')
                # Ensure we have the expected number of parts
                if len(parts) == 7:
                    security_type = parts[3]  # 4th column is the security type
                    # Filter for only records where the security type is 'EQ'
                    record = {
                            'rec_type': parts[0], #'Record Type'
                            'sr_no': parts[1], #'Sr No'
                            'security': parts[2], #'Name of Security'
                            'sec_type': parts[3], #'Security Type'
                            'qty_traded': int(parts[4]), #'Quantity Traded'
                            'qty_del': int(parts[5]), # 'Deliverable Quantity'
                            'perc_del_to_trd_qty': float(parts[6].strip()) # 'Perc  of Deliverable Quantity to Traded Quantity'
                        }
            
                    data.append(record)
                else:
                    # print(f"Skipping malformed line: {line}")  # Optional: log the malformed line
                    print(f"Skipping Header (NSE Delivery Data)")  # Optional: log the malformed line

    # Create a DataFrame from the list of dictionaries

    df_ndel = pd.DataFrame(data)
    
    df_ncsv = pd.read_csv(file_nb_csv)
    if 'TckrSymb' in df_ncsv.columns:
        # Do nothing since TckrSymb exists, we don't need to print anything
        # Proceed with your logic since TckrSymb is available
        # print("Column 'TckrSymb' found, proceeding with the program.")
        pass
    elif 'SYMBOL' in df_ncsv.columns:
        # rename SYMBOL to TckrSymb
        df_ncsv.rename(columns={'SYMBOL': 'TckrSymb'}, inplace=True)
    else:
         # No action needed, just pass
         print("Column 'TckrSymb' or 'SYMBOL' not found in the DataFrame.")
         print("Please check as it is esseintial part.")
         print('----------------------')
        # Do nothing if neither 'TckrSymb' nor 'SYMBOL' exists

    flag = check_nse_dates(df_ncsv,file_nd_dat)
    if flag =='True':
        df_ncsv.columns = df_ncsv.columns.str.lower()
        
        # Merge the two DataFrames on the specified columns
        merged_df_nse = pd.merge(df_ncsv, df_ndel, left_on='tckrsymb', right_on='security', how='left')
        # Specify the columns to keep
        nse_columns_to_keep = ['traddt','fininstrmid','fininstrmnm','isin','src','tckrsymb','sctysrs','opnpric',
                               'hghpric','lwpric','clspric','lastpric','prvsclsgpric','sttlmpric',
                               'ttltradgvol','ttltrfval','ttlnboftxsexctd','qty_del']
        # Keep only the specified columns
        merged_df_nse = merged_df_nse[nse_columns_to_keep]
        # merged_df_nse.columns
        merged_df_nse['delvry_trnovr']= ((merged_df_nse['ttltrfval'] / merged_df_nse['ttltradgvol']) * merged_df_nse['qty_del']).round(2)
        # merged_df_nse.reset_index(drop=True, inplace=True)
        # merged_df_nse.columns
        merged_df_nse = merged_df_nse.groupby(['isin', 'sctysrs'], as_index=False).agg({
                                        'qty_del':  'sum','delvry_trnovr': 'sum', 'src':'first',
                                        'traddt': 'first', 'isin': 'first','fininstrmid': 'first',  'tckrsymb': 'first',
                                        'sctysrs': 'first','fininstrmnm': 'first', 'opnpric': 'first',
                                        'hghpric': 'first','lwpric'	: 'first','clspric': 'first','lastpric': 'first',
                                        'prvsclsgpric'	: 'first','sttlmpric': 'first','ttltradgvol': 'first',
                                        'ttltrfval': 'first','ttlnboftxsexctd': 'first'
                                        })

        # put del data column in the last
        # Get the current column order
        current_columns = merged_df_nse.columns.tolist()

        # Create a new list of columns, moving 'qty_del' and 'delivery_turnover' to the last
        new_columns_order = [col for col in current_columns if col not in ['qty_del', 'delvry_trnovr']] + ['qty_del', 'delvry_trnovr']

        # Reorder the DataFrame columns
        merged_df_nse = merged_df_nse[new_columns_order]
        merged_df_nse['sctysrs'] = merged_df_nse['sctysrs'].fillna('N/A')
        # List of values to be removed
        values_to_remove = ['GS',	'GB',	'TB',	'SG',	'N0',	'N1',	'N2',	'N3',	'N4',	'N5',	'N6',	'N7',	'N8',	'N9',	'E1','X1',
                            'Y0',	'Y1',	'Y2',	'Y3',	'Y4',	'Y5',	'Y6',	'Y7',	'Y8', 'YA',	'YC',	'YG',	'YI',	'YK',	'YL',	'YM',	'YN',	'YO',	'YP',	'YQ',	'YR',	'YS',	'YT',	'YV',	'YW',	'YX',	'YY',	'YZ',	'ZY',								
                            'Z0',	'Z2',	'Z3',	'Z4',	'Z5',	'Z6',	'Z7',	'Z8',	'Z9', 'ZB',	'    ZC',	'ZD',	'ZE',	'ZF',	'ZG',	'ZH',	'ZI',	'ZJ',	'ZK',	'ZL',	'ZM',	'ZN',	'ZP',	'ZR',	'ZS',	'ZT',	'ZU',	'ZW',	'ZX',
                            'NB',	'NC',	'ND',	'NE',	'NF',	'NG',	'NH',	'NI',	'NJ',	'NK',	'NL',	'NM',	'NN',	'NO',	'NP',	'NQ',	'NR', 	'NS',	'NT',	'NU',	'NV',	'NW',	'NX',	'NY',	'NZ',			
                            'AB',	'AC',	'AG',	'AH',	'AI',	'AJ',	'AL',	'AN',	'AO',	'AP',	'AR',	'AT',	'AV',	'AW',	'AX',	'AY',	'AZ']

# Use isin() to filter out the specified values
        merged_df_nse = merged_df_nse[~merged_df_nse['sctysrs'].isin(values_to_remove)]
        merged_df_nse.reset_index(drop=True, inplace=True)
        # BE & BZ type sctysrs are not for intraday so all trades are delivered
        #  if merged_df_nse['qty_del'] is 0, put merged_df_nse['ttltradgvol'] data to merged_df_nse['qty_del']
        # Update qty_del where it is 0 with values from ttltradgvol
        
        # merged_df_nse.loc[merged_df_nse['qty_del'] == 0, 'qty_del'] = merged_df_nse['ttltradgvol']
        # merged_df_nse.loc[merged_df_nse['delvry_trnovr'] == 0, 'delvry_trnovr'] = merged_df_nse['ttltrfval']
        
        # Update qty_del based on the conditions
        condition = (merged_df_nse['sctysrs'].isin(['BE', 'BZ'])) & (merged_df_nse['qty_del'] == 0)
        merged_df_nse.loc[condition, 'qty_del'] = merged_df_nse['ttltradgvol']
        merged_df_nse.loc[condition, 'delvry_trnovr'] = merged_df_nse['ttltrfval']

        # Create a sub DataFrame where 'sctysrs' is not in the specified values
        excluded_values = ['BE', 'BZ', 'EQ', 'SM', 'ST','MF', 'ME']
        sub_df = merged_df_nse[~merged_df_nse['sctysrs'].isin(excluded_values)]
        sub_df.reset_index(drop=True, inplace=True)
        # sub_df.to_csv('sub_df.csv', index=False)
        # Filter the DataFrame
        filtered_df = merged_df_nse[merged_df_nse['sctysrs'].isin(excluded_values)]
        filtered_df.reset_index(drop=True, inplace=True)

        # Group by 'tckrsymb' and sum the specified columns
        # result = sub_df.groupby('tckrsymb').agg({'ttltradgvol': 'sum','ttltrfval': 'sum',
        #                                          'ttlnboftxsexctd': 'sum'}).reset_index()
        
        # data = {'tckrsymb': ['TRENT', 'TRENT'],
        #         'ttltradgvol': [1300,	1200],
        #         'ttltrfval': [1200,1000],
        #         'ttlnboftxsexctd': [1, 10],
        #         'sctysrs': ['Bl', 'P1']        }
        # data = pd.DataFrame(data)
        sub_df = sub_df.groupby('tckrsymb', as_index=False).agg(
                                    ttltradgvol=('ttltradgvol', 'sum'),
                                    ttltrfval=('ttltrfval', 'sum'),
                                    ttlnboftxsexctd=('ttlnboftxsexctd', 'sum'),
                                    othr_trds=('sctysrs', lambda x: ', '.join(x))
                                )
        
        
        # filtered_df[['othr_trds','othr_trds_vol','othr_trds_val','ttlnboftxsexctd']] = None, None, None, None
        filtered_df1 = filtered_df.copy()
        # Merge result_df data into filtered_df based on matching 'tckrsymb'
        filtered_df1 = filtered_df.merge(sub_df, on='tckrsymb', how='left', suffixes=('', '_y'))
        # Rename specific columns by passing a dictionary
        filtered_df1.rename(columns={'othr_trds_y': 'othr_trds', 'ttltradgvol_y': 'othr_trds_vol','ttltrfval_y': 'othr_trds_val','ttlnboftxsexctd_y': 'othr_trds_txsexctd'}, inplace=True)
        
        
        # filtered_df1.to_csv('filtered_df1.csv', index=False)

        # ---------------------------------------
        merged_df_nse = filtered_df1.copy()
        # As of Now leaving all the symbols that are not matching to main filtered_df
        # ----------------------------------------------------







        

        




        

        # Check if any symbol is 2 time with different delivry qty
        # Count occurrences of each value in column 'A'
        value_counts = merged_df_nse['tckrsymb'].value_counts()
        # Filter for duplicates (values that occur more than once)
        duplicates_nse = value_counts[value_counts > 1]
        # Assuming duplicates_nse is a pandas Series
        if len(duplicates_nse) != 0:
            for ticker in duplicates_nse.index:
                
                ticker_df  = merged_df_nse[merged_df_nse['tckrsymb']==ticker]
                # ticker_df.columns
                print(ticker_df[['traddt','isin','fininstrmid', 'tckrsymb', 'sctysrs', 
                'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del','delvry_trnovr' ]])
                # ticker_df.shape[0] > 1
                print("Merged_df_nse have more than 1 entry for above Symbols")
                print(ticker)
                
                if ticker_df['qty_del'].nunique() == 1:
                    # Remove rows where 'tckrsymb' is 'RADIOCITY' and 'sctysrs' is not 'EQ'
                    # Ask user for confirmation (optional)
                    
                    remove_confirm = input("Do you want to remove one of these entries? (Enter or yes/no): ")
                    if remove_confirm.lower() == 'yes' or remove_confirm == '':
                        # Drop one of the entries with sctysrs 'EQ'
                        # Remove rows where 'tckrsymb' is equal to ticker and 'sctysrs' is not 'EQ'
                        # merged_df_nse = merged_df_nse[~((merged_df_nse['tckrsymb'] == ticker) & (merged_df_nse['sctysrs'] == 'EQ') & (merged_df_nse.index == ticker_df.index[0]))]
                        merged_df_nse = merged_df_nse[~((merged_df_nse['tckrsymb'] == ticker) & (merged_df_nse['sctysrs'] != 'EQ'))]
                        print(f"Removed one entry for {ticker}.")
                        
                    elif remove_confirm.lower() == 'no':
                        print(f"Different qty_del for {ticker}: {ticker_df['qty_del'].unique()}")
                        
                    else:
                        print("Invalid input.")
                        merged_df_nse = merged_df_nse[~((merged_df_nse['tckrsymb'] == ticker) & (merged_df_nse['sctysrs'] != 'EQ'))]
                        print(f"Removed one entry for {ticker}.")
                        


        # check if any isin is 2 times
        isin_counts = merged_df_nse['isin'].value_counts()
        isin_duplicates = isin_counts[isin_counts > 1]
        if len(isin_duplicates) != 0:
            for isin in isin_duplicates.index:
                isin_df  = merged_df_nse[merged_df_nse['isin']==isin]
                print(isin_df[['traddt','isin','fininstrmid', 'tckrsymb', 'sctysrs', 
                'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del','delvry_trnovr' ]])
                print("Merged_df_nse have more than 1 entry for above ISINs")
                print(isin)
                # isin_df.shape[0] > 1
                print('Plan to something with such cases later')


        return merged_df_nse
        # merged_df_nse.to_csv('New_merged_nse.csv')
    else:
        print('Dates Mismatch for NSE Files')
        print('Check Files')
        print(f'Bhavcopy NSE {file_nb_csv}')
        print(f'Delivery NSE {file_nd_dat}')
            
          
# ---------------------------------------
def read_bse_files(file_bb_csv,file_bd_txt):
        
    bse_csv = pd.read_csv(file_bb_csv)
    bse_csv.columns = bse_csv.columns + '_b'
    bse_csv.columns = bse_csv.columns.str.lower()
    # bse_csv.columns
    
    # Specify the column names and their data types
    column_names = ['DATE', 'SCRIP CODE', 'DELIVERY QTY', 'DELIVERY VAL', 'DAY\'S VOLUME', 'DAY\'S TURNOVER', 'DELV. PER.']
    dtypes = {'DATE': str, 'SCRIP CODE': str, 'DELIVERY QTY': int, 'DELIVERY VAL': int, 'DAY\'S VOLUME': str, 'DAY\'S TURNOVER': str, 'DELV. PER.': str}

    try:
        # Read the file using read_csv
        bse_txt = pd.read_csv(file_bd_txt, sep='|', dtype=dtypes)
        bse_txt.columns = bse_txt.columns.str.lower()
        # bse_txt = pd.read_csv(file_bd_txt, sep='|', names=column_names, dtype=dtypes)
        # bse_txt = pd.read_csv(file_bd_txt, delimiter='|')
        # bse_txt.columns
    # print(df)
    except FileNotFoundError:
        print(f"The specified BSE file does not exist - {file_bd_txt}.")
    except pd.errors.ParserError:
        print(f"Error parsing the file {file_bd_txt}. Please check the data format.")
    
    # Renaming the columns
    bse_txt.rename(columns={
        'scrip code': 'scrip_code',
        'delivery qty': 'qty_del_b',
        'delivery val' : 'delvry_trnovr_b',
        "day's volume": "day_volume_b",
        "day's turnover": "day_trnover_b",
        "delv. per.": "day_perc_b"
    }, inplace=True)    

    bse_csv_dt= bse_csv['traddt_b'][0]
    bse_del_dt= bse_txt['date'][0]
    # Convert bse_del_dt from int64 to string in DDMMYYYY format
    bse_del_dt_str = str(bse_del_dt)

    # Convert del_date into a datetime object
    bse_del_date = datetime.strptime(bse_del_dt_str, '%d%m%Y').strftime('%Y-%m-%d')

    # Now compare the two dates
    if bse_del_date == bse_csv_dt:
        # print("The dates are the same for BSE Files.")
        b_flag = 'True'
    else:
        print("The dates are not same for BSE Files.")
        b_flag = 'Flase'
    
    if b_flag == 'True':
        # bse_csv.columns
        # bse_txt.columns
        bse_csv['fininstrmid_b'] = bse_csv['fininstrmid_b'].astype(str)
        # Merge the two DataFrames on the specified columns
        merged_df_bse = pd.merge(bse_csv, bse_txt, left_on='fininstrmid_b', right_on='scrip_code', how='left')
        # merged_df_bse.columns
        # merged_df_bse.to_csv('merged_df_bse.csv')
        bse_columns_to_keep = ['traddt_b','src_b','fininstrmid_b','isin_b','fininstrmnm_b', 'tckrsymb_b',
                               'sctysrs_b','opnpric_b','hghpric_b','lwpric_b','clspric_b','lastpric_b',
                               'prvsclsgpric_b','sttlmpric_b', 'ttltradgvol_b','ttltrfval_b','ttlnboftxsexctd_b',
                               'qty_del_b','delvry_trnovr_b','day_volume_b','day_trnover_b','day_perc_b']

        # Keep only the specified columns
        merged_df_bse = merged_df_bse[bse_columns_to_keep]
        merged_df_bse['sctysrs_b'] = merged_df_bse['sctysrs_b'].fillna('N/A')
        # merged_df_nse[merged_df_nse['tckrsymb'] == "RADIOCITY"]['sctysrs']

        # merged_df_bse.to_csv('New_merged_bse.csv')
        # Check Duplicate in merged_df_bse having same tckrsymb_b
        value_counts = merged_df_bse['tckrsymb_b'].value_counts()
        duplicates_bse = value_counts[value_counts > 1]
        if len(duplicates_bse) != 0:
            for ticker in duplicates_bse.index:
                
                ticker_df  = merged_df_bse[merged_df_bse['tckrsymb_b']==ticker]
                # ticker_df.columns
                print(ticker_df[['traddt_b','isin_b','fininstrmid_b', 'tckrsymb_b', 'sctysrs_b', 
                'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b','delvry_trnovr_b' ]])
                # ticker_df.shape[0] > 1
                print("Merged_df_bse have more than 1 entry for above Symbols")
                print(ticker)
                if ticker_df['qty_del_b'].nunique() == 1:
                    # Remove rows where 'tckrsymb' is 'RADIOCITY' and 'sctysrs' is not 'EQ'
                    # Ask user for confirmation (optional)
                    remove_confirm = input("Do you want to remove one of these entries? (yes/no): ")
                    if remove_confirm.lower() == 'yes':
                        # Drop one of the entries with sctysrs 'EQ'
                        # merged_df_nse = merged_df_nse[~((merged_df_nse['tckrsymb'] == ticker) & (merged_df_nse['sctysrs'] == 'EQ') & (merged_df_nse.index == ticker_df.index[0]))]
                        merged_df_bse = merged_df_bse[~((merged_df_bse['tckrsymb_b'] == ticker) & (merged_df_bse['sctysrs_b'] != 'EQ'))]
                        print(f"Removed one entry for {ticker}.")
                else:
                    print(f"Different qty_del for {ticker}: {ticker_df['qty_del_b'].unique()}")        


        isin_counts = merged_df_bse['isin_b'].value_counts()
        isin_duplicates = isin_counts[isin_counts > 1]
        if len(isin_duplicates) != 0:
            for isin in isin_duplicates.index:
                # print(isin)
                ticker_df  = merged_df_bse[merged_df_bse['isin_b']==isin]
                # ticker_df.columns
                print(ticker_df[['traddt_b','isin_b','fininstrmid_b', 'tckrsymb_b', 'sctysrs_b', 
                'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b','delvry_trnovr_b' ]])
                # ticker_df.shape[0] > 1
                print("Merged_df_bse have more than 1 entry for above ISIN")
                print(isin)
                if ticker_df['qty_del_b'].nunique() == 1:
                    # Remove rows where 'tckrsymb' is 'RADIOCITY' and 'sctysrs' is not 'EQ'
                    # Ask user for confirmation (optional)
                    remove_confirm = input("Do you want to remove one of these entries? (yes/no): ")
                    if remove_confirm.lower() == 'yes':
                        # Drop one of the entries with sctysrs 'EQ'
                        # merged_df_nse = merged_df_nse[~((merged_df_nse['tckrsymb'] == ticker) & (merged_df_nse['sctysrs'] == 'EQ') & (merged_df_nse.index == ticker_df.index[0]))]
                        merged_df_bse = merged_df_bse[~((merged_df_bse['isin_b'] == isin) & (merged_df_bse['qty_del_b'].isna()))]

                        print(f"Removed one entry for {isin} where Delivery Quantity is NaN.")
                else:
                    print(f"Different qty_del for {ticker}: {ticker_df['qty_del_b'].unique()}")        



        values_to_remove = ['G','F']
        merged_df_bse = merged_df_bse[~merged_df_bse['sctysrs_b'].isin(values_to_remove)]
        merged_df_bse.reset_index(drop=True, inplace=True)
        # merged_df_bse.to_csv('merged_df_bse.csv')
        return merged_df_bse
    else:
        print('Dates Mismatch for BSE Files')
        print('Check Files')
        print(f'Bhavcopy BSE {file_bb_csv}')
        print(f'Delivery BSE {file_bd_txt}')
def get_combine_calc(combined_df_temp):
    date1 = combined_df_temp['traddt'].unique()[0]
    date2 = combined_df_temp['traddt_b'].unique()[0]
    if date1 == date2 or date1 != date2:
            # combined_df_new.to_csv('combined_df_new1.csv')
        combined_df_temp['traddt'] = combined_df_temp['traddt'].fillna(combined_df_temp['traddt_b'])
        # combined_df_temp['isin_b'] =  combined_df_temp['isin_b'] + '_BSE'
        combined_df_temp['isin'] = combined_df_temp['isin'].fillna(combined_df_temp['isin_b'])
        combined_df_temp['opnpric'] = combined_df_temp['opnpric'].fillna(combined_df_temp['opnpric_b'])
        combined_df_temp['hghpric'] = combined_df_temp['hghpric'].fillna(combined_df_temp['hghpric_b'])
        combined_df_temp['lwpric'] = combined_df_temp['lwpric'].fillna(combined_df_temp['lwpric_b'])
        combined_df_temp['clspric'] = combined_df_temp['clspric'].fillna(combined_df_temp['clspric_b'])
        combined_df_temp['prvsclsgpric'] = combined_df_temp['prvsclsgpric'].fillna(combined_df_temp['prvsclsgpric_b'])
        combined_df_temp['sttlmpric'] = combined_df_temp['sttlmpric'].fillna(combined_df_temp['sttlmpric_b'])
        combined_df_temp['lastpric'] = combined_df_temp['lastpric'].fillna(combined_df_temp['lastpric_b'])
        combined_df_temp['fininstrmnm'] = combined_df_temp['fininstrmnm'].fillna(combined_df_temp['fininstrmnm_b'])
        # combined_df_temp.columns    
        # Not summing if symbol is not vailable in BSE or NSE
        combined_df_temp['sum_ttltradgvol'] = combined_df_temp['ttltradgvol'].fillna(0) + combined_df_temp['ttltradgvol_b'].fillna(0)
        combined_df_temp['sum_ttltrfval'] = combined_df_temp['ttltrfval'].fillna(0) + combined_df_temp['ttltrfval_b'].fillna(0)
        combined_df_temp['sum_ttlnboftxsexctd'] = combined_df_temp['ttlnboftxsexctd'].fillna(0) + combined_df_temp['ttlnboftxsexctd_b'].fillna(0)
        combined_df_temp['sum_del_qty'] = combined_df_temp['qty_del'].fillna(0) + combined_df_temp['qty_del_b'].fillna(0)
        combined_df_temp['sum_delvry_trnovr'] = combined_df_temp['delvry_trnovr'].fillna(0) + combined_df_temp['delvry_trnovr_b'].fillna(0)
        combined_df_temp['del_per'] =  (combined_df_temp['sum_del_qty'] / combined_df_temp['sum_ttltradgvol'] *100).round(2)
        # avg price  = total turnover / total trades qty
        
        combined_df_temp['avg_price'] = (combined_df_temp['sum_ttltrfval'] / combined_df_temp['sum_ttltradgvol']).round(2)
        # avg qty per order  =total qty / total trades
        combined_df_temp['avg_qty_per_order'] = (combined_df_temp['sum_ttltradgvol'] / combined_df_temp['sum_ttlnboftxsexctd'] ).round(2)
        # avg order price = avg price * avg qty per order
        combined_df_temp['avg_order_price'] = (combined_df_temp['avg_price'] * combined_df_temp['avg_qty_per_order']).round(2) 
        # close price to avg price
        combined_df_temp['close_to_avg']  = (combined_df_temp['avg_price'] - combined_df_temp['clspric']).round(2)
        # close price to avg price percent
        combined_df_temp['close_avg_perc'] = (combined_df_temp['close_to_avg']*100 /  combined_df_temp['clspric']).round(2)    
        # combined_df_temp.columns

        # columns_to_keep = ['traddt','isin','fininstrmid','fininstrmid_b','tckrsymb','tckrsymb_b','sctysrs',
        #                     'sctysrs_b','src','src_b','fininstrmnm','opnpric','hghpric','lwpric','clspric',
        #                     'lastpric','prvsclsgpric','sttlmpric','ttltradgvol','ttltrfval','ttlnboftxsexctd',
        #                     'qty_del','delvry_trnovr','ttltradgvol_b','ttltrfval_b','ttlnboftxsexctd_b','qty_del_b',
        #                     'delvry_trnovr_b','sum_ttltradgvol','sum_ttltrfval','sum_ttlnboftxsexctd','sum_del_qty',
        #                     'sum_delvry_trnovr','del_per','avg_price','avg_qty_per_order','avg_order_price',
        #                     'close_to_avg',	'close_avg_perc']
        # combined_df_new = combined_df_temp[columns_to_keep] 
        combined_df_temp = combined_df_temp[['traddt','isin','fininstrmid','fininstrmid_b','tckrsymb','tckrsymb_b','sctysrs',
                            'sctysrs_b','src','src_b','fininstrmnm','opnpric','hghpric','lwpric','clspric',
                            'lastpric','prvsclsgpric','sttlmpric','ttltradgvol','ttltrfval','ttlnboftxsexctd',
                            'qty_del','delvry_trnovr','ttltradgvol_b','ttltrfval_b','ttlnboftxsexctd_b','qty_del_b',
                            'delvry_trnovr_b','sum_ttltradgvol','sum_ttltrfval','sum_ttlnboftxsexctd','sum_del_qty',
                            'sum_delvry_trnovr','del_per','avg_price','avg_qty_per_order','avg_order_price',
                            'close_to_avg',	'close_avg_perc',
                            'othr_trds','othr_trds_vol','othr_trds_val','othr_trds_txsexctd']]

        
        
        combined_df_new = combined_df_temp.copy().reset_index(drop=True)

        # combined_df_new['tckrsymb']
    else:
        print('Date Mismatch in NSE BSE Combined data')
        print(f'Date:1-- {date1} & Date:2-- {date2}')
    

    return combined_df_new  
def combine_bse_nse(file_nd_dat,file_nb_csv,file_bb_csv, file_bd_txt):

    merged_df_nse = read_nse_files(file_nd_dat,file_nb_csv)
    merged_df_bse = read_bse_files(file_bb_csv,file_bd_txt)
    # merged_df_bse['isin_b']
    # merged_df_bse.to_csv('merged_df_bse.csv')
    combined_df_temp = pd.merge(merged_df_nse, merged_df_bse, left_on='isin', right_on='isin_b', how='outer')
    # convert combined_df_temp['fininstrmid'] to integer
    combined_df_temp['fininstrmid'] = (combined_df_temp['fininstrmid'].fillna(0).astype(int) ).astype(str)
    # Replace '0' (as a string) with NaN
    combined_df_temp['fininstrmid'] = combined_df_temp['fininstrmid'].replace('0', np.nan)
    combined_df_new = get_combine_calc(combined_df_temp)
    
    # combined_df_new.to_csv('combine_df_NEW1.csv')

    
    # return combined_df_new, daily_data_df
    return combined_df_new


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

def update_static_data(combined_df_new):
    static_data_clmns = ['isin','fininstrmid','fininstrmid_b','tckrsymb','tckrsymb_b','src','src_b','fininstrmnm']
    static_df = combined_df_new[static_data_clmns]
    # ensure static_df['fininstrmid'] & static_df['fininstrmid_b'] are unique
    # Check each row to ensure at least one column has valid data
    # for index, row in static_df.iterrows():
    #     fininstrmid_valid = pd.notna(row['fininstrmid']) and row['fininstrmid'] != '' and row['fininstrmid'] != 0
    #     fininstrmid_b_valid = pd.notna(row['fininstrmid_b']) and row['fininstrmid_b'] != '' and row['fininstrmid_b'] != 0
        
    #     if not (fininstrmid_valid or fininstrmid_b_valid):
    #         print(f"Both columns are invalid for ISIN '{row['isin']}' at index {index}: "
    #             f"fininstrmid: {row['fininstrmid']}, fininstrmid_b: {row['fininstrmid_b']}")

    # Check conditions for 'fininstrmid' and 'fininstrmid_b'
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

def update_daily_data(combined_df_new):
    # combined_df_new.columns
    
    columns_to_drop = ['fininstrmid', 'fininstrmid_b','tckrsymb', 'tckrsymb_b','src', 'src_b', 'fininstrmnm']
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



def get_data_app(updt_df,data_month,data_year):
    data_year= str(data_year)
    data_month = f"{data_month:02d}"
    for index, row in updt_df.iterrows():
        d_date = row['Date_type']  # Get the date as a string
        print(d_date)
        # bhavcopy_date = row['bhavcopy_Date']  # Get the date as a string
        # delivery_date = row['delivery_Date']  # Get the date as a string
        # old_date = row['old_date']
        # year_val= row['year']
        # dtmonth = row['dtmonth']
        data_day = d_date.day
        data_day=f"{data_day:02d}"
        # break
        file_nd_dat, file_bd_txt, file_nb_csv, file_bb_csv = get_bhavcopy_path(data_day, data_month, data_year)
        combined_df_new = combine_bse_nse(file_nd_dat,file_nb_csv,file_bb_csv, file_bd_txt)
        # combined_df_new.to_csv('initail_setup.csv')
        # print('Initial setip file created')
        missing_data = update_static_data(combined_df_new)
        daily_data_df = update_daily_data(combined_df_new)
    
    return 


def update_database_function(update_start_date, update_end_date):
    # print('from databse function')
    # print(update_start_date, update_end_date)
    # print('----------------')
    # convert start_date, end_date to datetime from text format '2024-11-05'
    update_start_date = datetime.strptime(update_start_date, '%Y-%m-%d')
    update_end_date = datetime.strptime(update_end_date, '%Y-%m-%d')
    print(update_start_date, update_end_date)
    
    update_start_date_day =update_start_date.day
    update_start_date_month = update_start_date.month
    update_start_date_year= update_start_date.year

    update_end_date_day =update_end_date.day
    update_end_date_month =update_end_date.month
    update_end_date_year= update_end_date.year
    print(update_start_date_month,update_end_date_month, update_start_date_year ,update_end_date_year)
    # print(type(update_start_date_month))

    # print(type( update_start_date_year))
    if (update_start_date_month != update_end_date_month) or (update_start_date_year != update_end_date_year):
        status = "Use One Month only at a time"
        print(status)
    else:
        dt_list = get_date_range(update_start_date_month, update_start_date_year)
        # dt_list = get_date_range(11, 2024)
        # update_start_date = 2024-11-21
        # update_end_date_day = 2024-11-22
        # Create the new DataFrame by filtering the original DataFrame
        
        updt_df = dt_list[(dt_list['Date_type'] >= update_start_date) & (dt_list['Date_type'] <= update_end_date)]
        get_data_app(updt_df,update_start_date_month,update_start_date_year)



        status = "OK"
    return status
# ----------------------------------------------------

