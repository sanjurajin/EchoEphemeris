from app import app
from flask import render_template, jsonify
from flask import request
from datetime import datetime
import time

#--------------------------------------------

#   DESHBOARD PAGE 

from app.id_utils import zx_64439cbd , x0f7092b4 
@app.route('/')
def home():
    charts_data = x0f7092b4()

    # x, y = equity_curve()
    eq_dates, equity = zx_64439cbd()

    return render_template('index.html',charts_data=charts_data, eq_dates=eq_dates, equity=equity)

# -------------------------------------------------

from app.exch_data import last_business_day, download_nse_bse_files_1, updates_dates

@app.route('/util_exch',methods=['GET', 'POST'])
def utility_exch():
        last_business_date  = last_business_day()
        # all_symbols = get_postgresdb_symbols()
        if request.method == 'POST':
            if 'today' in request.form:
                today = datetime.now().strftime('%d/%m/%Y')
                start_date = today
                end_date = today
                print(f'Data will be downloaded from {start_date} to {end_date}.')
                status = download_nse_bse_files_1(start_date, end_date)

            elif 'range' in request.form:
                start_date = request.form['start_date']
                end_date = request.form['end_date']
                print(f'Data will be downloaded from {start_date} to {end_date}.')
                status = download_nse_bse_files_1(start_date, end_date)

        
            elif 'update_dates' in request.form:
                new_dates = request.form.get('dates').strip().splitlines()
                if not new_dates or new_dates == ['']:  # Check for empty input
                    print("No dates were provided.")
                else:
                    print(f"New dates received: {new_dates}")
                    status = updates_dates(new_dates)
            # Handle the database update case
        #     elif 'update_range' in request.form:
        #         update_start_date = request.form['update_start_date']
        #         update_end_date = request.form['update_end_date']
        #         print(f'Database will be updated from {update_start_date} to {update_end_date}.')
        #         status = update_database_function(update_start_date, update_end_date)  # Replace with your actual function
        #     elif 'select_company' in request.form:
        #         selected_comp = request.form['symbol']
        #         print(selected_comp)
        #     elif 'symbol' in request.form:
        #          symbol = request.form.get('symbol')
        #          print(f'Selected symbol is {symbol}.')
                
        return render_template('util_exch.html', last_business_date = last_business_date)
        

# -------------------------------------------------


# --------------------------------------------------------
from app.exch_data import  get_postgresdb_data, get_postgresdb_symbols

@app.route('/basic_analysis_n',  methods=['GET', 'POST'])
def basic_analysis_n():
    all_symbols = get_postgresdb_symbols()
    if 'symbol' in request.form:
        symbol = request.form.get('symbol')
        print(f'Selected symbol is {symbol}.')
    else:
        symbol = 'ICICIBANK :- ICICI BANK LTD.  INE090A01021'
        
    isin_daily_df, script_name = get_postgresdb_data(symbol)
    
    return render_template('basic_analysis_n.html',isin_daily_df = isin_daily_df.to_dict(orient='records'), all_symbols = all_symbols, script_name=script_name)
