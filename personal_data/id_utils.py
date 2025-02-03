
import pandas as pd
import os
from datetime import datetime, timedelta
import calendar
import json
import numpy as np
import openpyxl as xl

cwd = os.getcwd()


def get_logbook():
    # Load the workbook
    new_trade_log = os.path.join('app', 'data','new_trade_log.xlsx')
    wb = xl.load_workbook(new_trade_log, data_only=True)
    sheet = wb['log_trds']
    # Extract the data from the worksheet
    data = sheet.values
    # Get the first row as the header
    columns = next(data)
    # Create a DataFrame with the extracted data and specified column names
    df = pd.DataFrame(data, columns=columns)
    # Display the DataFrame
    print(df)
    return df

def convert_data_types():
    
    # data_path = os.path.join('data', 'new_trade_log.csv') # New Trade Log File
    # data_path = os.path.join(cwd,f"app/data/new_trade_log.csv")
    # df = pd.read_csv(data_path)
    df = get_logbook()
    # df.columns
    # make a sub dataframe with all the data where trade_type is Open
    df_open = df[df['trade_type'] == 'Open']
    # remove all the data from df where trade_type is Open
    df = df[df['trade_type'] != 'Open']

    
    # Convert date columns to datetime.date
    df['date_open'] = pd.to_datetime(df['date_open'], format='%d-%m-%Y').dt.date
    df['date_close'] = pd.to_datetime(df['date_close'], format='%d-%m-%Y').dt.date
    
    # Convert time columns to datetime.time
    df['time_open'] = pd.to_datetime(df['time_open'], format='%H:%M:%S').dt.time
    df['time_close'] = pd.to_datetime(df['time_close'], format='%H:%M:%S').dt.time
    
    # Convert qty columns to integer
    df['open_qty'] = df['open_qty'].astype(int)
    df['close_qty'] = df['close_qty'].astype(int)
    df['wk'] = df['wk'].astype(int)
    
    # Convert specific columns to string (text)
    
    text_columns = ['script_type', 'symbol', 'trade_type', 'script', 'side_open', 'side_close']
    for col in text_columns:
        df[col] = df[col].astype(str)  # Ensure they are treated as strings
    
    price_columns = ['open_price', 'close_price', 'total_open_pr', 'total_close_pr', 'pnl'] # New Trade Log File
    # for col in price_columns:
    #     if df[col].dtype == 'object':
    #         df[col] = df[col].str.replace(',',",", '').astype(float)
        
  
            
    # Convert price columns to float
    
    for col in price_columns:
        df[col] = df[col].astype(float)
    
    df['date_open'] = pd.to_datetime(df['date_open'])
    
    
    df.sort_index(inplace=True)
    
    df = df[['date_open','symbol', 'script_type', 'script', 'trade_type',
            'wk', 'open_qty', 'open_price', 'total_open_pr',
            'date_close','close_qty', 'close_price','total_close_pr', 'pnl'
        
            ]]
    return df


#   Function for last trades data retrieval and creating chart data
def generate_dday_pnljs(pnl_label, pnl_values, td_date):
   
    last_pnljs = {
        'labels': pnl_label,
        'datasets': [
            {
                'label': td_date,
                'data': pnl_values,
                'borderColor': 'rgb(75, 192, 192)',
                'tension': 0.1
            }
        ]
    }
    
    return last_pnljs



def get_last_trades():
    data = convert_data_types()
    # data['open'] = data.index
    
    # last_days = data['date_open'].dt.date.unique()[-6:][0]
    last_days = data['date_open'].dt.date.unique()[-4:][0]
    data_n = data[data['date_open'].dt.date >= last_days]
    all_dates = data_n['date_open'].dt.date.unique()
    
    charts_data = []
    
    for date in all_dates:
        data_1 = data[data['date_open'].dt.date == date]
        symbols = data_1['symbol'].tolist()
        pnl_values = data_1['pnl'].tolist()
        # crate sum for data_1['pnl]
        data_1_sum = data_1['pnl'].sum().round(1)  

        td_date = data_1['date_open'].dt.strftime('%d-%m-%Y').tolist()[0]  + " (Rs.:" + str(data_1_sum)+")"
      
       

        chart_data = generate_dday_pnljs(symbols, pnl_values, td_date)
        # --------------------------
        # add foirst value as zero and its label
        # Modify the data to start from zero
        modified_data = [0] + chart_data['datasets'][0]['data']  # Add 0 at the beginning
        chart_data['datasets'][0]['data'] = modified_data  # Update the dataset

        # Update labels to reflect the new data
        modified_labels = ['0'] + chart_data['labels']  # Add a label for the starting point
        chart_data['labels'] = modified_labels  # Update the labels
        charts_data.append(chart_data)


    return charts_data

# ------------------------------------


# Equity Curve


def equity_curve():
    trade_log = convert_data_types()
    # Convert 'Date' column to datetime format
    # trade_log['open'] = trade_log.index
    # trade_log['Date'] = pd.to_datetime(trade_log['Date'])

    # Calculate daily PnL
    
    daily_pnl = trade_log.groupby('date_open')['pnl'].sum().reset_index()

    # Initial investment
    initial_investment = 10000  # Rs. 1 lakh

    # Calculate Equity Curve
    daily_pnl['Equity'] = (initial_investment + daily_pnl['pnl'].cumsum()).round(2)

    # Prepare data for the chart
    eq_dates = daily_pnl['date_open'].dt.strftime('%Y-%m-%d').tolist()  # Format dates
    equity = daily_pnl['Equity'].tolist()

    return eq_dates, equity

