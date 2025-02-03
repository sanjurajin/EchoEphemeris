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
x33b379a7 = os.path.join(cwd, 'app', 'creden.txt')
var_631aada4 = open(x33b379a7, 'r').read().split()
_b4c16391 = var_631aada4[0]
xa0e71f8f = var_631aada4[1]
_2e48e91c = var_631aada4[2]
_b4c16391 = 'echoephemeris'
_2e48e91c = 'aurvaprah'

def var_49e00907():
    db_config = {'dbname': _b4c16391, 'user': xa0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}'
    engine = create_engine(conn_string)
    query = '\n    SELECT *\n    FROM my_static_data\n    '
    try:
        var_f1016f82 = pd.read_sql(query, engine)
    except Exception as e:
        print(f'An error occurred: {e}')
    finally:
        engine.dispose()
        print('Database connection closed.')
    var_f1016f82.columns
    var_f1016f82['tckrsymb'] = var_f1016f82['tckrsymb'].fillna(var_f1016f82['tckrsymb_b'])
    var_f1016f82 = var_f1016f82.sort_values(by='fininstrmnm')
    var_f1016f82 = var_f1016f82.reset_index()
    _9e99a581 = var_f1016f82[['tckrsymb', 'fininstrmnm', 'isin']]
    xce535d10 = pd.DataFrame()
    xce535d10['symbol_name'] = _9e99a581['tckrsymb'] + ' :- ' + _9e99a581['fininstrmnm'] + '  ' + _9e99a581['isin']
    sA_40025d04 = xce535d10['symbol_name'].tolist()
    return sA_40025d04

def sA_57414141(var_b76a7ca1):
    try:
        sA_d887db09 = var_b76a7ca1.split(':-')
        var_5b5f25e9 = sA_d887db09[0].strip()
        zx_fe80de81 = sA_d887db09[1].strip().rsplit(' ', 1)[0]
        zx_7813612d = sA_d887db09[1].strip().rsplit(' ', 1)[1]
        db_config = {'dbname': _b4c16391, 'user': xa0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
        conn_string = f'postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}'
        engine = create_engine(conn_string, connect_args={'connect_timeout': 10})
        query = '\n            SELECT * FROM my_daily_data\n            WHERE isin = %(isin)s\n        '
        with engine.connect().execution_options(timeout=30) as connection:
            zx_f33bad8d = pd.read_sql(query, connection, params={'isin': zx_7813612d})
        if zx_f33bad8d.empty:
            print(f'No data found for ISIN: {zx_7813612d}')
            return (pd.DataFrame(), 'None')
        zx_f33bad8d['traddt'] = pd.to_datetime(zx_f33bad8d['traddt'], dayfirst=True)
        zx_f33bad8d['time'] = zx_f33bad8d['traddt']
        zx_f33bad8d = zx_f33bad8d.sort_values(by='time')
        zx_f33bad8d = zx_f33bad8d.drop(columns=['traddt'])
        zx_6c4662bd = zx_f33bad8d['sum_delvry_trnovr'].sum()
        var_42e21f40 = zx_f33bad8d['sum_ttltrfval'].sum()
        _87651205 = zx_6c4662bd * 100 / var_42e21f40
        zx_f33bad8d['avg_del_perc'] = _87651205.round(4)
        var_67b2649f = zx_f33bad8d['avg_order_price'].mean().round(2)
        zx_f33bad8d['avg_order_price'] = (zx_f33bad8d['avg_order_price'] / 1000).round(3)
        zx_f33bad8d['avg_of_aop'] = var_67b2649f
        zx_f33bad8d['avg_of_aop'] = (zx_f33bad8d['avg_of_aop'] / 1000).round(3)
        zx_f33bad8d['sum_delvry_trnovr'] = (zx_f33bad8d['sum_delvry_trnovr'] / 1000000).round(3)
        zx_f33bad8d.rename(columns={'opnpric': 'open', 'hghpric': 'high', 'lwpric': 'low', 'clspric': 'close'}, inplace=True)
        zx_f33bad8d = zx_f33bad8d.reindex(columns=['time', 'open', 'high', 'low', 'close', 'sum_delvry_trnovr', 'del_per', 'avg_order_price', 'avg_del_perc', 'sum_del_qty', 'avg_of_aop', 'events'])
        return (zx_f33bad8d, zx_fe80de81)
    except SQLAlchemyError as e:
        print(f'Database error occurred: {str(e)}')
        return (pd.DataFrame(), 'None')
    except Exception as e:
        print(f'An unexpected error occurred: {str(e)}')
        return (pd.DataFrame(), 'None')
    finally:
        try:
            engine.dispose()
            print('Database connection closed.')
        except:
            pass
x6e998b78 = os.path.join(cwd, 'app', 'data', 'exch_files')

def xa2cc7414():
    var_d5d49ccf = os.path.join(cwd, 'app', 'data', 'exch_files', 'calander_file.csv')
    x9f481db4 = pd.read_csv(var_d5d49ccf)
    sA_2f6adb17 = x9f481db4[-1:].values[0]
    sA_2f6adb17 = pd.to_datetime(sA_2f6adb17[0], format='%d-%b-%y').strftime('%d-%m-%Y')
    return sA_2f6adb17

def _e4d70624(sA_0987903c):
    zx_67392fd3 = pd.DataFrame(sA_0987903c, columns=['Date'])
    zx_67392fd3['Date'] = pd.to_datetime(zx_67392fd3['Date'], format='%d-%m-%Y', dayfirst=True).dt.strftime('%d-%b-%y')
    var_d5d49ccf = os.path.join(cwd, 'app', 'data', 'exch_files', 'calander_file.csv')
    zx_67392fd3.to_csv(var_d5d49ccf, mode='a', header=False, index=False)
    _073c1634 = 'Date Data File Updated'
    return _073c1634

def xd8d918a9(var_ee01b9df, _24938c07):
    print(var_ee01b9df, _24938c07)
    var_ee01b9df = datetime.strptime(var_ee01b9df, '%Y-%m-%d')
    _24938c07 = datetime.strptime(_24938c07, '%Y-%m-%d')
    if var_ee01b9df == _24938c07 or var_ee01b9df < _24938c07:
        zx_65808d74 = var_ee01b9df.month
        var_400bb302 = var_ee01b9df.year
        zx_e3c9de4d = var_a64995fe(zx_65808d74, var_400bb302)
        zx_e3c9de4d['Date_type'] = pd.to_datetime(zx_e3c9de4d['Date_type'])
        zx_e3c9de4d = zx_e3c9de4d[(zx_e3c9de4d['Date_type'] >= var_ee01b9df) & (zx_e3c9de4d['Date_type'] <= _24938c07)]
        if not zx_e3c9de4d.empty:
            for index, row in zx_e3c9de4d.iterrows():
                var_acf3357e = row['Date_type']
                zx_995b5cdf = row['bhavcopy_Date']
                xab710224 = row['delivery_Date']
                zx_469f4308 = row['old_date']
                xcb705957 = row['year']
                _f96a6410 = row['dtmonth']
                print('--------------------------')
                print(row['Date'])
                try:
                    sA_372ff396(zx_995b5cdf, xab710224)
                except Exception as e:
                    print(f'Error downloading NSE files for {zx_995b5cdf}: {e}')
                try:
                    var_877a6e13(zx_995b5cdf, zx_469f4308, xcb705957, _f96a6410)
                except Exception as e:
                    print(f'Error downloading NSE files for {zx_995b5cdf}: {e}')
                time.sleep(10)
            _073c1634 = 'Data Downloaded Successfully'
        else:
            print('No Dates found between start_date and end_date in Calender File')
            _073c1634 = 'No Dates found between start_date and end_date in Calender File'
    else:
        print('start_date is greater than end_date')
        _073c1634 = 'start_date is greater than end_date'
    return _073c1634

def var_a64995fe(zx_65808d74, var_400bb302):
    var_d5d49ccf = os.path.join(cwd, 'app', 'data', 'exch_files', 'calander_file.csv')
    x9f481db4 = pd.read_csv(var_d5d49ccf)
    x9f481db4['bhavcopy_Date'] = pd.to_datetime(x9f481db4['Date'], format='%d-%b-%y').dt.strftime('%Y%m%d')
    x9f481db4['delivery_Date'] = pd.to_datetime(x9f481db4['Date'], format='%d-%b-%y').dt.strftime('%d%m%Y')
    x9f481db4['Date_type'] = pd.to_datetime(x9f481db4['Date'], format='%d-%b-%y')
    x9f481db4['old_date'] = pd.to_datetime(x9f481db4['Date'], format='%d-%b-%y').dt.strftime('%d%m%y')
    x9f481db4['year'] = pd.to_datetime(x9f481db4['Date'], format='%d-%b-%y').dt.year
    x9f481db4['date'] = pd.to_datetime(x9f481db4['Date'], format='%d-%b-%y').dt.day
    x9f481db4['month'] = pd.to_datetime(x9f481db4['Date'], format='%d-%b-%y').dt.month
    x9f481db4['dtmonth'] = x9f481db4['date'].astype(str).str.zfill(2) + x9f481db4['month'].astype(str).str.zfill(2)
    x30ba289b = x9f481db4[(x9f481db4['Date_type'].dt.month == zx_65808d74) & (x9f481db4['Date_type'].dt.year == var_400bb302)]
    return x30ba289b

def sA_372ff396(zx_995b5cdf, xab710224):
    zx_c1c8d369 = f'https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{zx_995b5cdf}_F_0000.csv.zip'
    x2d3a708a = f'https://nsearchives.nseindia.com/archives/equities/mto/MTO_{xab710224}.DAT'
    zx_378f37b1 = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    var_a9f4b3d2 = requests.get(zx_c1c8d369, headers=zx_378f37b1)
    sA_7c11f08a = os.path.join(x6e998b78)
    x0085e1d6 = os.path.join(x6e998b78, f'BhavCopy_NSE_CM_0_0_0_{zx_995b5cdf}_F_0000.csv.zip')
    if var_a9f4b3d2.status_code == 200:
        with open(x0085e1d6, 'wb') as file:
            file.write(var_a9f4b3d2.content)
        with zipfile.ZipFile(x0085e1d6, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(x6e998b78, 'NSE Files'))
        print('Bhavcopy_NSE completed successfully!')
        os.remove(x0085e1d6)
        zx_5fb059b2 = True
    else:
        var_239f59ed = pd.read_csv('https://archives.nseindia.com/products/content/sec_bhavdata_full_' + xab710224 + '.csv')
        if not var_239f59ed.empty:
            var_239f59ed.columns = var_239f59ed.columns.str.strip()
            var_239f59ed['turnover'] = var_239f59ed['AVG_PRICE'] * var_239f59ed['TTL_TRD_QNTY']
            var_239f59ed.drop(columns=['TURNOVER_LACS'], inplace=True)
            var_3c1cd05a = os.path.join(x6e998b78, 'NSE Files', f'BhavCopy_NSE_CM_0_0_0_{zx_995b5cdf}_F_0000_O.csv')
            var_239f59ed.to_csv(var_3c1cd05a, index=False)
            print('Bhavcopy_NSE Old completed successfully!')
            zx_5fb059b2 = True
        else:
            print(f'Failed to download NSE file.')
    if zx_5fb059b2:
        var_a9f4b3d2 = requests.get(x2d3a708a, headers=zx_378f37b1)
        if var_a9f4b3d2.status_code == 200:
            var_3c1cd05a = os.path.join(x6e998b78, 'NSE Files', f'MTO_{xab710224}.DAT')
            with open(var_3c1cd05a, 'wb') as file:
                file.write(var_a9f4b3d2.content)
            print('Delivery NSE completed successfully!')
        else:
            print(f'Failed to download Delivery NSE file.')
    else:
        print(f'Failed to download NSE Bhavcopy file, skipping Delivery.')

def var_877a6e13(zx_995b5cdf, zx_469f4308, xcb705957, _f96a6410):
    x80f67872 = f'https://www.bseindia.com//download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{zx_995b5cdf}_F_0000.CSV'
    zx_378f37b1 = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    var_a9f4b3d2 = requests.get(x80f67872, headers=zx_378f37b1)
    sA_7c11f08a = os.path.join(x6e998b78)
    file = os.path.join(x6e998b78, 'BSE Files', f'BhavCopy_BSE_CM_0_0_0_{zx_995b5cdf}_F_0000.csv')
    if var_a9f4b3d2.status_code == 200:
        with open(file, 'wb') as file:
            file.write(var_a9f4b3d2.content)
        print('Bhavcopy_BSE completed successfully!')
    else:
        x3ba85c2f = f'https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{zx_469f4308}.zip'
        var_a9f4b3d2 = requests.get(x3ba85c2f, headers=zx_378f37b1)
        if var_a9f4b3d2.status_code == 200:
            file = os.path.join(x6e998b78, 'BSE Files', f'BhavCopy_BSE_CM_0_0_0_{zx_995b5cdf}_F_0000_O.csv')
            with open(file, 'wb') as file:
                file.write(var_a9f4b3d2.content)
            print('Bhavcopy_BSE Old completed successfully!')
        else:
            print(f'Failed to download BSE Bhavcopy file')
    sA_ec2f02ea = f'https://www.bseindia.com/BSEDATA/gross/{xcb705957}/SCBSEALL{_f96a6410}.zip'
    var_a9f4b3d2 = requests.get(sA_ec2f02ea, headers=zx_378f37b1)
    if var_a9f4b3d2.status_code == 200:
        sA_c8542847 = os.path.join(x6e998b78, 'BSE Files', f'SCBSEALL{zx_995b5cdf}.zip')
        _581c6bab = os.path.join(x6e998b78, 'BSE Files')
        with open(sA_c8542847, 'wb') as file:
            file.write(var_a9f4b3d2.content)
        with zipfile.ZipFile(sA_c8542847, 'r') as zip_ref:
            zip_ref.extractall(_581c6bab)
            x4d66d8d0 = zip_ref.namelist()
        if x4d66d8d0:
            xe505cfb4 = x4d66d8d0[0]
            zx_dddcec93 = f'SCBSEALL{zx_995b5cdf}.txt'
            _c15b5de8 = os.path.join(_581c6bab, xe505cfb4)
            var_a7ea9bf4 = os.path.join(_581c6bab, zx_dddcec93)
            os.rename(_c15b5de8, var_a7ea9bf4)
            xc58a8b33 = os.path.join(_581c6bab, sA_c8542847)
            os.remove(xc58a8b33)
            print('Delivery BSE completed successfully!')
        else:
            print(f'Failed to download Delivery BSE file')
    else:
        print(f'Failed to download Delivery BSE file')