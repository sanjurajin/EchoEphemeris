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
if os.getlogin() == 'sanju':
    x33b379a7 = os.path.join(cwd, 'personal_data', 'creden.txt')
    zx_631aada4 = open(x33b379a7, 'r').read().split()
    _b4c16391 = zx_631aada4[0]
    sa_a0e71f8f = zx_631aada4[1]
    _2e48e91c = zx_631aada4[2]
else:
    x33b379a7 = os.path.join(cwd, 'app', 'creden.txt')
    zx_631aada4 = open(x33b379a7, 'r').read().split()
    _b4c16391 = zx_631aada4[0]
    sa_a0e71f8f = zx_631aada4[1]
    _2e48e91c = zx_631aada4[2]

def sa_49e00907():
    var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{var_4780fc62['user']}:{var_4780fc62['password']}@{var_4780fc62['host']}:{var_4780fc62['port']}/{var_4780fc62['dbname']}'
    engine = create_engine(conn_string)
    query = '\n    SELECT *\n    FROM my_static_data\n    '
    try:
        zx_f1016f82 = pd.read_sql(query, engine)
    except Exception as e:
        print(f'An error occurred: {e}')
    finally:
        engine.dispose()
        print('Database connection closed.')
    zx_f1016f82.columns
    zx_f1016f82['tckrsymb'] = zx_f1016f82['tckrsymb'].fillna(zx_f1016f82['tckrsymb_b'])
    zx_f1016f82 = zx_f1016f82.sort_values(by='fininstrmnm')
    zx_f1016f82 = zx_f1016f82.reset_index()
    sa_9e99a581 = zx_f1016f82[['tckrsymb', 'fininstrmnm', 'isin']]
    zx_ce535d10 = pd.DataFrame()
    zx_ce535d10['symbol_name'] = sa_9e99a581['tckrsymb'] + ' :- ' + sa_9e99a581['fininstrmnm'] + '  ' + sa_9e99a581['isin']
    var_40025d04 = zx_ce535d10['symbol_name'].tolist()
    return var_40025d04

def zx_57414141(sa_b76a7ca1):
    try:
        zx_d887db09 = sa_b76a7ca1.split(':-')
        var_5b5f25e9 = zx_d887db09[0].strip()
        xfe80de81 = zx_d887db09[1].strip().rsplit(' ', 1)[0]
        var_7813612d = zx_d887db09[1].strip().rsplit(' ', 1)[1]
        var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
        conn_string = f'postgresql+psycopg2://{var_4780fc62['user']}:{var_4780fc62['password']}@{var_4780fc62['host']}:{var_4780fc62['port']}/{var_4780fc62['dbname']}'
        engine = create_engine(conn_string, connect_args={'connect_timeout': 10})
        query = '\n            SELECT * FROM my_daily_data\n            WHERE isin = %(isin)s\n        '
        with engine.connect().execution_options(timeout=30) as connection:
            xf33bad8d = pd.read_sql(query, connection, params={'isin': var_7813612d})
        if xf33bad8d.empty:
            print(f'No data found for ISIN: {var_7813612d}')
            return (pd.DataFrame(), 'None')
        xf33bad8d['traddt'] = pd.to_datetime(xf33bad8d['traddt'], dayfirst=True)
        xf33bad8d['time'] = xf33bad8d['traddt']
        xf33bad8d = xf33bad8d.sort_values(by='time')
        xf33bad8d = xf33bad8d.drop(columns=['traddt'])
        var_6c4662bd = xf33bad8d['sum_delvry_trnovr'].sum()
        zx_42e21f40 = xf33bad8d['sum_ttltrfval'].sum()
        _87651205 = var_6c4662bd * 100 / zx_42e21f40
        xf33bad8d['avg_del_perc'] = _87651205.round(4)
        _67b2649f = xf33bad8d['avg_order_price'].mean().round(2)
        xf33bad8d['avg_order_price'] = (xf33bad8d['avg_order_price'] / 1000).round(3)
        xf33bad8d['avg_of_aop'] = _67b2649f
        xf33bad8d['avg_of_aop'] = (xf33bad8d['avg_of_aop'] / 1000).round(3)
        xf33bad8d['sum_delvry_trnovr'] = (xf33bad8d['sum_delvry_trnovr'] / 1000000).round(3)
        xf33bad8d.rename(columns={'opnpric': 'open', 'hghpric': 'high', 'lwpric': 'low', 'clspric': 'close'}, inplace=True)
        xf33bad8d = xf33bad8d.reindex(columns=['time', 'open', 'high', 'low', 'close', 'sum_delvry_trnovr', 'del_per', 'avg_order_price', 'avg_del_perc', 'sum_del_qty', 'avg_of_aop', 'events'])
        return (xf33bad8d, xfe80de81)
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
sa_6e998b78 = os.path.join(cwd, 'app', 'data', 'exch_files')

def var_a2cc7414():
    var_d5d49ccf = os.path.join(cwd, 'app', 'data', 'exch_files', 'calander_file.csv')
    _9f481db4 = pd.read_csv(var_d5d49ccf)
    zx_2f6adb17 = _9f481db4[-1:].values[0]
    zx_2f6adb17 = pd.to_datetime(zx_2f6adb17[0], format='%d-%b-%y').strftime('%d-%m-%Y')
    return zx_2f6adb17

def xe4d70624(x0987903c):
    sa_67392fd3 = pd.DataFrame(x0987903c, columns=['Date'])
    sa_67392fd3['Date'] = pd.to_datetime(sa_67392fd3['Date'], format='%d-%m-%Y', dayfirst=True).dt.strftime('%d-%b-%y')
    var_d5d49ccf = os.path.join(cwd, 'app', 'data', 'exch_files', 'calander_file.csv')
    sa_67392fd3.to_csv(var_d5d49ccf, mode='a', header=False, index=False)
    zx_073c1634 = 'Date Data File Updated'
    return zx_073c1634

def sa_d8d918a9(var_ee01b9df, sa_24938c07):
    print(var_ee01b9df, sa_24938c07)
    var_ee01b9df = datetime.strptime(var_ee01b9df, '%Y-%m-%d')
    sa_24938c07 = datetime.strptime(sa_24938c07, '%Y-%m-%d')
    if var_ee01b9df == sa_24938c07 or var_ee01b9df < sa_24938c07:
        sa_65808d74 = var_ee01b9df.month
        sa_400bb302 = var_ee01b9df.year
        xe3c9de4d = sa_a64995fe(sa_65808d74, sa_400bb302)
        xe3c9de4d['Date_type'] = pd.to_datetime(xe3c9de4d['Date_type'])
        xe3c9de4d = xe3c9de4d[(xe3c9de4d['Date_type'] >= var_ee01b9df) & (xe3c9de4d['Date_type'] <= sa_24938c07)]
        if not xe3c9de4d.empty:
            for index, row in xe3c9de4d.iterrows():
                _acf3357e = row['Date_type']
                x995b5cdf = row['bhavcopy_Date']
                _ab710224 = row['delivery_Date']
                var_469f4308 = row['old_date']
                sa_cb705957 = row['year']
                zx_f96a6410 = row['dtmonth']
                print('--------------------------')
                print(row['Date'])
                try:
                    _372ff396(x995b5cdf, _ab710224)
                except Exception as e:
                    print(f'Error downloading NSE files for {x995b5cdf}: {e}')
                try:
                    sa_877a6e13(x995b5cdf, var_469f4308, sa_cb705957, zx_f96a6410)
                except Exception as e:
                    print(f'Error downloading NSE files for {x995b5cdf}: {e}')
                time.sleep(10)
            zx_073c1634 = 'Data Downloaded Successfully'
        else:
            print('No Dates found between start_date and end_date in Calender File')
            zx_073c1634 = 'No Dates found between start_date and end_date in Calender File'
    else:
        print('start_date is greater than end_date')
        zx_073c1634 = 'start_date is greater than end_date'
    return zx_073c1634

def sa_a64995fe(sa_65808d74, sa_400bb302):
    var_d5d49ccf = os.path.join(cwd, 'app', 'data', 'exch_files', 'calander_file.csv')
    _9f481db4 = pd.read_csv(var_d5d49ccf)
    _9f481db4['bhavcopy_Date'] = pd.to_datetime(_9f481db4['Date'], format='%d-%b-%y').dt.strftime('%Y%m%d')
    _9f481db4['delivery_Date'] = pd.to_datetime(_9f481db4['Date'], format='%d-%b-%y').dt.strftime('%d%m%Y')
    _9f481db4['Date_type'] = pd.to_datetime(_9f481db4['Date'], format='%d-%b-%y')
    _9f481db4['old_date'] = pd.to_datetime(_9f481db4['Date'], format='%d-%b-%y').dt.strftime('%d%m%y')
    _9f481db4['year'] = pd.to_datetime(_9f481db4['Date'], format='%d-%b-%y').dt.year
    _9f481db4['date'] = pd.to_datetime(_9f481db4['Date'], format='%d-%b-%y').dt.day
    _9f481db4['month'] = pd.to_datetime(_9f481db4['Date'], format='%d-%b-%y').dt.month
    _9f481db4['dtmonth'] = _9f481db4['date'].astype(str).str.zfill(2) + _9f481db4['month'].astype(str).str.zfill(2)
    x30ba289b = _9f481db4[(_9f481db4['Date_type'].dt.month == sa_65808d74) & (_9f481db4['Date_type'].dt.year == sa_400bb302)]
    return x30ba289b

def _372ff396(x995b5cdf, _ab710224):
    xc1c8d369 = f'https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{x995b5cdf}_F_0000.csv.zip'
    sa_2d3a708a = f'https://nsearchives.nseindia.com/archives/equities/mto/MTO_{_ab710224}.DAT'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    response = requests.get(xc1c8d369, headers=headers)
    zip_file_path = os.path.join(sa_6e998b78)
    zip_file = os.path.join(sa_6e998b78, f'BhavCopy_NSE_CM_0_0_0_{x995b5cdf}_F_0000.csv.zip')
    if response.status_code == 200:
        with open(zip_file, 'wb') as file:
            file.write(response.content)
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(sa_6e998b78, 'NSE Files'))
        print('Bhavcopy_NSE completed successfully!')
        os.remove(zip_file)
        _5fb059b2 = True
    else:
        payload = pd.read_csv('https://archives.nseindia.com/products/content/sec_bhavdata_full_' + _ab710224 + '.csv')
        if not payload.empty:
            payload.columns = payload.columns.str.strip()
            payload['turnover'] = payload['AVG_PRICE'] * payload['TTL_TRD_QNTY']
            payload.drop(columns=['TURNOVER_LACS'], inplace=True)
            x3c1cd05a = os.path.join(sa_6e998b78, 'NSE Files', f'BhavCopy_NSE_CM_0_0_0_{x995b5cdf}_F_0000_O.csv')
            payload.to_csv(x3c1cd05a, index=False)
            print('Bhavcopy_NSE Old completed successfully!')
            _5fb059b2 = True
        else:
            print(f'Failed to download NSE file.')
    if _5fb059b2:
        response = requests.get(sa_2d3a708a, headers=headers)
        if response.status_code == 200:
            x3c1cd05a = os.path.join(sa_6e998b78, 'NSE Files', f'MTO_{_ab710224}.DAT')
            with open(x3c1cd05a, 'wb') as file:
                file.write(response.content)
            print('Delivery NSE completed successfully!')
        else:
            print(f'Failed to download Delivery NSE file.')
    else:
        print(f'Failed to download NSE Bhavcopy file, skipping Delivery.')

def sa_877a6e13(x995b5cdf, var_469f4308, sa_cb705957, zx_f96a6410):
    var_80f67872 = f'https://www.bseindia.com//download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{x995b5cdf}_F_0000.CSV'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    response = requests.get(var_80f67872, headers=headers)
    zip_file_path = os.path.join(sa_6e998b78)
    file = os.path.join(sa_6e998b78, 'BSE Files', f'BhavCopy_BSE_CM_0_0_0_{x995b5cdf}_F_0000.csv')
    if response.status_code == 200:
        with open(file, 'wb') as file:
            file.write(response.content)
        print('Bhavcopy_BSE completed successfully!')
    else:
        _3ba85c2f = f'https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{var_469f4308}.zip'
        response = requests.get(_3ba85c2f, headers=headers)
        if response.status_code == 200:
            file = os.path.join(sa_6e998b78, 'BSE Files', f'BhavCopy_BSE_CM_0_0_0_{x995b5cdf}_F_0000_O.csv')
            with open(file, 'wb') as file:
                file.write(response.content)
            print('Bhavcopy_BSE Old completed successfully!')
        else:
            print(f'Failed to download BSE Bhavcopy file')
    _ec2f02ea = f'https://www.bseindia.com/BSEDATA/gross/{sa_cb705957}/SCBSEALL{zx_f96a6410}.zip'
    response = requests.get(_ec2f02ea, headers=headers)
    if response.status_code == 200:
        _c8542847 = os.path.join(sa_6e998b78, 'BSE Files', f'SCBSEALL{x995b5cdf}.zip')
        _581c6bab = os.path.join(sa_6e998b78, 'BSE Files')
        with open(_c8542847, 'wb') as file:
            file.write(response.content)
        with zipfile.ZipFile(_c8542847, 'r') as zip_ref:
            zip_ref.extractall(_581c6bab)
            _4d66d8d0 = zip_ref.namelist()
        if _4d66d8d0:
            zx_e505cfb4 = _4d66d8d0[0]
            sa_dddcec93 = f'SCBSEALL{x995b5cdf}.txt'
            xc15b5de8 = os.path.join(_581c6bab, zx_e505cfb4)
            var_a7ea9bf4 = os.path.join(_581c6bab, sa_dddcec93)
            os.rename(xc15b5de8, var_a7ea9bf4)
            zx_c58a8b33 = os.path.join(_581c6bab, _c8542847)
            os.remove(zx_c58a8b33)
            print('Delivery BSE completed successfully!')
        else:
            print(f'Failed to download Delivery BSE file')
    else:
        print(f'Failed to download Delivery BSE file')

def zx_a679377a(sa_41834621, sa_65808d74, sa_400bb302):
    if type(sa_41834621) == int:
        sa_41834621 = f'{sa_41834621:02d}'
    if type(sa_65808d74) == int:
        sa_65808d74 = f'{sa_65808d74:02d}'
    var_feb4c676 = os.path.join(cwd, 'app', 'data', 'exch_files', 'NSE Files')
    x3dca8be8 = os.path.join(cwd, 'app', 'data', 'exch_files', 'BSE Files')
    _4c51300a = os.path.join(var_feb4c676, f'MTO_{sa_41834621}{sa_65808d74}{sa_400bb302}.DAT')
    zx_eb105c7a = os.path.join(x3dca8be8, f'SCBSEALL{sa_400bb302}{sa_65808d74}{sa_41834621}.txt')
    zx_5b518483 = os.path.join(var_feb4c676, f'BhavCopy_NSE_CM_0_0_0_{sa_400bb302}{sa_65808d74}{sa_41834621}_F_0000.csv')
    x13bf367f = os.path.join(var_feb4c676, f'BhavCopy_NSE_CM_0_0_0_{sa_400bb302}{sa_65808d74}{sa_41834621}_F_0000_O.csv')
    var_204fa89d = os.path.join(x3dca8be8, f'BhavCopy_BSE_CM_0_0_0_{sa_400bb302}{sa_65808d74}{sa_41834621}_F_0000.csv')
    _896d97d0 = os.path.join(x3dca8be8, f'BhavCopy_BSE_CM_0_0_0_{sa_400bb302}{sa_65808d74}{sa_41834621}_F_0000_O.csv')
    if os.path.isfile(zx_5b518483):
        var_cd8bc1aa = zx_5b518483
    elif os.path.isfile(x13bf367f):
        var_cd8bc1aa = x13bf367f
    else:
        print('Neither New nor Old NSE Bhavcopy file is available.')
        var_cd8bc1aa = ''
    if os.path.isfile(var_204fa89d):
        _334f7a3d = var_204fa89d
    elif os.path.isfile(_896d97d0):
        _334f7a3d = _896d97d0
    else:
        print('Neither New nor Old BSE Bhavcopy file is available.')
        _334f7a3d = ''
    return (_4c51300a, zx_eb105c7a, var_cd8bc1aa, _334f7a3d)

def sa_fddfefa4(var_4be6b81c, _4c51300a):
    if 'TradDt' in var_4be6b81c.columns:
        var_78c8768c = var_4be6b81c['TradDt'][0]
    elif 'DATE1' in var_4be6b81c.columns:
        var_78c8768c = var_4be6b81c['DATE1'][0].strip()
        var_78c8768c = datetime.strptime(var_78c8768c, '%d-%b-%Y').strftime('%Y-%m-%d')
    else:
        var_78c8768c = None
    with open(_4c51300a, 'r') as file:
        var_5ea44c39 = file.readlines()
        if len(var_5ea44c39) > 1:
            _3672f82d = var_5ea44c39[1].strip()
            _89445ea0 = _3672f82d.split(',')
            if len(_89445ea0) > 2:
                zx_8a8455d9 = _89445ea0[2]
        _2bc292cd = zx_8a8455d9
        _2bc292cd = datetime.strptime(_2bc292cd, '%d%m%Y').strftime('%Y-%m-%d')
    if _2bc292cd == var_78c8768c:
        sa_807d0fbc = 'True'
    else:
        sa_807d0fbc = 'Flase'
    return sa_807d0fbc

def xfa20b436(_4c51300a, var_cd8bc1aa):
    zx_3a6eb079 = []
    with open(_4c51300a, 'r') as file:
        for _ in range(3):
            next(file)
        for sa_38a9c1e7 in file:
            sa_38a9c1e7 = sa_38a9c1e7.strip()
            if sa_38a9c1e7:
                zx_d887db09 = sa_38a9c1e7.split(',')
                if len(zx_d887db09) == 7:
                    sa_d23df3af = zx_d887db09[3]
                    var_70ce871f = {'rec_type': zx_d887db09[0], 'sr_no': zx_d887db09[1], 'security': zx_d887db09[2], 'sec_type': zx_d887db09[3], 'qty_traded': int(zx_d887db09[4]), 'qty_del': int(zx_d887db09[5]), 'perc_del_to_trd_qty': float(zx_d887db09[6].strip())}
                    zx_3a6eb079.append(var_70ce871f)
                else:
                    print(f'Skipping Header (NSE Delivery Data)')
    x74e9446e = pd.DataFrame(zx_3a6eb079)
    var_4be6b81c = pd.read_csv(var_cd8bc1aa)
    if 'TckrSymb' in var_4be6b81c.columns:
        pass
    elif 'SYMBOL' in var_4be6b81c.columns:
        var_4be6b81c.rename(columns={'SYMBOL': 'TckrSymb'}, inplace=True)
    else:
        print("Column 'TckrSymb' or 'SYMBOL' not found in the DataFrame.")
        print('Please check as it is esseintial part.')
        print('----------------------')
    sa_807d0fbc = sa_fddfefa4(var_4be6b81c, _4c51300a)
    if sa_807d0fbc == 'True':
        var_4be6b81c.columns = var_4be6b81c.columns.str.lower()
        _a23a1d2f = pd.merge(var_4be6b81c, x74e9446e, left_on='tckrsymb', right_on='security', how='left')
        _fe9f5edb = ['traddt', 'fininstrmid', 'fininstrmnm', 'isin', 'src', 'tckrsymb', 'sctysrs', 'opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric', 'prvsclsgpric', 'sttlmpric', 'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del']
        _a23a1d2f = _a23a1d2f[_fe9f5edb]
        _a23a1d2f['delvry_trnovr'] = (_a23a1d2f['ttltrfval'] / _a23a1d2f['ttltradgvol'] * _a23a1d2f['qty_del']).round(2)
        _a23a1d2f = _a23a1d2f.groupby(['isin', 'sctysrs'], as_index=False).agg({'qty_del': 'sum', 'delvry_trnovr': 'sum', 'src': 'first', 'traddt': 'first', 'isin': 'first', 'fininstrmid': 'first', 'tckrsymb': 'first', 'sctysrs': 'first', 'fininstrmnm': 'first', 'opnpric': 'first', 'hghpric': 'first', 'lwpric': 'first', 'clspric': 'first', 'lastpric': 'first', 'prvsclsgpric': 'first', 'sttlmpric': 'first', 'ttltradgvol': 'first', 'ttltrfval': 'first', 'ttlnboftxsexctd': 'first'})
        zx_c7c9c9b8 = _a23a1d2f.columns.tolist()
        zx_912072f1 = [col for col in zx_c7c9c9b8 if col not in ['qty_del', 'delvry_trnovr']] + ['qty_del', 'delvry_trnovr']
        _a23a1d2f = _a23a1d2f[zx_912072f1]
        _a23a1d2f['sctysrs'] = _a23a1d2f['sctysrs'].fillna('N/A')
        sa_5c10987b = ['GS', 'GB', 'TB', 'SG', 'N0', 'N1', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9', 'E1', 'X1', 'Y0', 'Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7', 'Y8', 'YA', 'YC', 'YG', 'YI', 'YK', 'YL', 'YM', 'YN', 'YO', 'YP', 'YQ', 'YR', 'YS', 'YT', 'YV', 'YW', 'YX', 'YY', 'YZ', 'ZY', 'Z0', 'Z2', 'Z3', 'Z4', 'Z5', 'Z6', 'Z7', 'Z8', 'Z9', 'ZB', '    ZC', 'ZD', 'ZE', 'ZF', 'ZG', 'ZH', 'ZI', 'ZJ', 'ZK', 'ZL', 'ZM', 'ZN', 'ZP', 'ZR', 'ZS', 'ZT', 'ZU', 'ZW', 'ZX', 'NB', 'NC', 'ND', 'NE', 'NF', 'NG', 'NH', 'NI', 'NJ', 'NK', 'NL', 'NM', 'NN', 'NO', 'NP', 'NQ', 'NR', 'NS', 'NT', 'NU', 'NV', 'NW', 'NX', 'NY', 'NZ', 'AB', 'AC', 'AG', 'AH', 'AI', 'AJ', 'AL', 'AN', 'AO', 'AP', 'AR', 'AT', 'AV', 'AW', 'AX', 'AY', 'AZ']
        _a23a1d2f = _a23a1d2f[~_a23a1d2f['sctysrs'].isin(sa_5c10987b)]
        _a23a1d2f.reset_index(drop=True, inplace=True)
        x596d40a4 = _a23a1d2f['sctysrs'].isin(['BE', 'BZ']) & (_a23a1d2f['qty_del'] == 0)
        _a23a1d2f.loc[x596d40a4, 'qty_del'] = _a23a1d2f['ttltradgvol']
        _a23a1d2f.loc[x596d40a4, 'delvry_trnovr'] = _a23a1d2f['ttltrfval']
        zx_7d2f4792 = ['BE', 'BZ', 'EQ', 'SM', 'ST', 'MF', 'ME']
        x558bae4c = _a23a1d2f[~_a23a1d2f['sctysrs'].isin(zx_7d2f4792)]
        x558bae4c.reset_index(drop=True, inplace=True)
        var_bc16acc0 = _a23a1d2f[_a23a1d2f['sctysrs'].isin(zx_7d2f4792)]
        var_bc16acc0.reset_index(drop=True, inplace=True)
        x558bae4c = x558bae4c.groupby('tckrsymb', as_index=False).agg(ttltradgvol=('ttltradgvol', 'sum'), ttltrfval=('ttltrfval', 'sum'), ttlnboftxsexctd=('ttlnboftxsexctd', 'sum'), othr_trds=('sctysrs', lambda x: ', '.join(x)))
        xed256730 = var_bc16acc0.copy()
        xed256730 = var_bc16acc0.merge(x558bae4c, on='tckrsymb', how='left', suffixes=('', '_y'))
        xed256730.rename(columns={'othr_trds_y': 'othr_trds', 'ttltradgvol_y': 'othr_trds_vol', 'ttltrfval_y': 'othr_trds_val', 'ttlnboftxsexctd_y': 'othr_trds_txsexctd'}, inplace=True)
        _a23a1d2f = xed256730.copy()
        sa_0abf75cb = _a23a1d2f['tckrsymb'].value_counts()
        xa5d9de3b = sa_0abf75cb[sa_0abf75cb > 1]
        if len(xa5d9de3b) != 0:
            for zx_2c6bc158 in xa5d9de3b.index:
                _5282482f = _a23a1d2f[_a23a1d2f['tckrsymb'] == zx_2c6bc158]
                print(_5282482f[['traddt', 'isin', 'fininstrmid', 'tckrsymb', 'sctysrs', 'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del', 'delvry_trnovr']])
                print('Merged_df_nse have more than 1 entry for above Symbols')
                print(zx_2c6bc158)
                if _5282482f['qty_del'].nunique() == 1:
                    x0860132f = input('Do you want to remove one of these entries? (Enter or yes/no): ')
                    if x0860132f.lower() == 'yes' or x0860132f == '':
                        _a23a1d2f = _a23a1d2f[~((_a23a1d2f['tckrsymb'] == zx_2c6bc158) & (_a23a1d2f['sctysrs'] != 'EQ'))]
                        print(f'Removed one entry for {zx_2c6bc158}.')
                    elif x0860132f.lower() == 'no':
                        print(f'Different qty_del for {zx_2c6bc158}: {_5282482f['qty_del'].unique()}')
                    else:
                        print('Invalid input.')
                        _a23a1d2f = _a23a1d2f[~((_a23a1d2f['tckrsymb'] == zx_2c6bc158) & (_a23a1d2f['sctysrs'] != 'EQ'))]
                        print(f'Removed one entry for {zx_2c6bc158}.')
        x00f317ee = _a23a1d2f['isin'].value_counts()
        sa_d4c4dcfd = x00f317ee[x00f317ee > 1]
        if len(sa_d4c4dcfd) != 0:
            for x48a4d578 in sa_d4c4dcfd.index:
                zx_7f6c6b1f = _a23a1d2f[_a23a1d2f['isin'] == x48a4d578]
                print(zx_7f6c6b1f[['traddt', 'isin', 'fininstrmid', 'tckrsymb', 'sctysrs', 'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del', 'delvry_trnovr']])
                print('Merged_df_nse have more than 1 entry for above ISINs')
                print(x48a4d578)
                print('Plan to something with such cases later')
        return _a23a1d2f
    else:
        print('Dates Mismatch for NSE Files')
        print('Check Files')
        print(f'Bhavcopy NSE {var_cd8bc1aa}')
        print(f'Delivery NSE {_4c51300a}')

def var_f2e352ac(_334f7a3d, zx_eb105c7a):
    x497f1234 = pd.read_csv(_334f7a3d)
    x497f1234.columns = x497f1234.columns + '_b'
    x497f1234.columns = x497f1234.columns.str.lower()
    x2000f1e8 = ['DATE', 'SCRIP CODE', 'DELIVERY QTY', 'DELIVERY VAL', "DAY'S VOLUME", "DAY'S TURNOVER", 'DELV. PER.']
    sa_010c9807 = {'DATE': str, 'SCRIP CODE': str, 'DELIVERY QTY': int, 'DELIVERY VAL': int, "DAY'S VOLUME": str, "DAY'S TURNOVER": str, 'DELV. PER.': str}
    try:
        zx_d5906e21 = pd.read_csv(zx_eb105c7a, sep='|', dtype=sa_010c9807)
        zx_d5906e21.columns = zx_d5906e21.columns.str.lower()
    except FileNotFoundError:
        print(f'The specified BSE file does not exist - {zx_eb105c7a}.')
    except pd.errors.ParserError:
        print(f'Error parsing the file {zx_eb105c7a}. Please check the data format.')
    zx_d5906e21.rename(columns={'scrip code': 'scrip_code', 'delivery qty': 'qty_del_b', 'delivery val': 'delvry_trnovr_b', "day's volume": 'day_volume_b', "day's turnover": 'day_trnover_b', 'delv. per.': 'day_perc_b'}, inplace=True)
    var_463a048b = x497f1234['traddt_b'][0]
    zx_3e2a8280 = zx_d5906e21['date'][0]
    _db8b2175 = str(zx_3e2a8280)
    zx_32e1b76e = datetime.strptime(_db8b2175, '%d%m%Y').strftime('%Y-%m-%d')
    if zx_32e1b76e == var_463a048b:
        var_b027a25f = 'True'
    else:
        print('The dates are not same for BSE Files.')
        var_b027a25f = 'Flase'
    if var_b027a25f == 'True':
        x497f1234['fininstrmid_b'] = x497f1234['fininstrmid_b'].astype(str)
        zx_0e3b8575 = pd.merge(x497f1234, zx_d5906e21, left_on='fininstrmid_b', right_on='scrip_code', how='left')
        xe5400f53 = ['traddt_b', 'src_b', 'fininstrmid_b', 'isin_b', 'fininstrmnm_b', 'tckrsymb_b', 'sctysrs_b', 'opnpric_b', 'hghpric_b', 'lwpric_b', 'clspric_b', 'lastpric_b', 'prvsclsgpric_b', 'sttlmpric_b', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b', 'day_volume_b', 'day_trnover_b', 'day_perc_b']
        zx_0e3b8575 = zx_0e3b8575[xe5400f53]
        zx_0e3b8575['sctysrs_b'] = zx_0e3b8575['sctysrs_b'].fillna('N/A')
        sa_0abf75cb = zx_0e3b8575['tckrsymb_b'].value_counts()
        x6e8f3834 = sa_0abf75cb[sa_0abf75cb > 1]
        if len(x6e8f3834) != 0:
            for zx_2c6bc158 in x6e8f3834.index:
                _5282482f = zx_0e3b8575[zx_0e3b8575['tckrsymb_b'] == zx_2c6bc158]
                print(_5282482f[['traddt_b', 'isin_b', 'fininstrmid_b', 'tckrsymb_b', 'sctysrs_b', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b']])
                print('Merged_df_bse have more than 1 entry for above Symbols')
                print(zx_2c6bc158)
                if _5282482f['qty_del_b'].nunique() == 1:
                    x0860132f = input('Do you want to remove one of these entries? (yes/no): ')
                    if x0860132f.lower() == 'yes':
                        zx_0e3b8575 = zx_0e3b8575[~((zx_0e3b8575['tckrsymb_b'] == zx_2c6bc158) & (zx_0e3b8575['sctysrs_b'] != 'EQ'))]
                        print(f'Removed one entry for {zx_2c6bc158}.')
                else:
                    print(f'Different qty_del for {zx_2c6bc158}: {_5282482f['qty_del_b'].unique()}')
        x00f317ee = zx_0e3b8575['isin_b'].value_counts()
        sa_d4c4dcfd = x00f317ee[x00f317ee > 1]
        if len(sa_d4c4dcfd) != 0:
            for x48a4d578 in sa_d4c4dcfd.index:
                _5282482f = zx_0e3b8575[zx_0e3b8575['isin_b'] == x48a4d578]
                print(_5282482f[['traddt_b', 'isin_b', 'fininstrmid_b', 'tckrsymb_b', 'sctysrs_b', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b']])
                print('Merged_df_bse have more than 1 entry for above ISIN')
                print(x48a4d578)
                if _5282482f['qty_del_b'].nunique() == 1:
                    x0860132f = input('Do you want to remove one of these entries? (yes/no): ')
                    if x0860132f.lower() == 'yes':
                        zx_0e3b8575 = zx_0e3b8575[~((zx_0e3b8575['isin_b'] == x48a4d578) & zx_0e3b8575['qty_del_b'].isna())]
                        print(f'Removed one entry for {x48a4d578} where Delivery Quantity is NaN.')
                else:
                    print(f'Different qty_del for {zx_2c6bc158}: {_5282482f['qty_del_b'].unique()}')
        sa_5c10987b = ['G', 'F']
        zx_0e3b8575 = zx_0e3b8575[~zx_0e3b8575['sctysrs_b'].isin(sa_5c10987b)]
        zx_0e3b8575.reset_index(drop=True, inplace=True)
        return zx_0e3b8575
    else:
        print('Dates Mismatch for BSE Files')
        print('Check Files')
        print(f'Bhavcopy BSE {_334f7a3d}')
        print(f'Delivery BSE {zx_eb105c7a}')

def sa_62b57292(xbc9ca89e):
    var_eeba8bb6 = xbc9ca89e['traddt'].unique()[0]
    x367ef122 = xbc9ca89e['traddt_b'].unique()[0]
    if var_eeba8bb6 == x367ef122 or var_eeba8bb6 != x367ef122:
        xbc9ca89e['traddt'] = xbc9ca89e['traddt'].fillna(xbc9ca89e['traddt_b'])
        xbc9ca89e['isin'] = xbc9ca89e['isin'].fillna(xbc9ca89e['isin_b'])
        xbc9ca89e['opnpric'] = xbc9ca89e['opnpric'].fillna(xbc9ca89e['opnpric_b'])
        xbc9ca89e['hghpric'] = xbc9ca89e['hghpric'].fillna(xbc9ca89e['hghpric_b'])
        xbc9ca89e['lwpric'] = xbc9ca89e['lwpric'].fillna(xbc9ca89e['lwpric_b'])
        xbc9ca89e['clspric'] = xbc9ca89e['clspric'].fillna(xbc9ca89e['clspric_b'])
        xbc9ca89e['prvsclsgpric'] = xbc9ca89e['prvsclsgpric'].fillna(xbc9ca89e['prvsclsgpric_b'])
        xbc9ca89e['sttlmpric'] = xbc9ca89e['sttlmpric'].fillna(xbc9ca89e['sttlmpric_b'])
        xbc9ca89e['lastpric'] = xbc9ca89e['lastpric'].fillna(xbc9ca89e['lastpric_b'])
        xbc9ca89e['fininstrmnm'] = xbc9ca89e['fininstrmnm'].fillna(xbc9ca89e['fininstrmnm_b'])
        xbc9ca89e['sum_ttltradgvol'] = xbc9ca89e['ttltradgvol'].fillna(0) + xbc9ca89e['ttltradgvol_b'].fillna(0)
        xbc9ca89e['sum_ttltrfval'] = xbc9ca89e['ttltrfval'].fillna(0) + xbc9ca89e['ttltrfval_b'].fillna(0)
        xbc9ca89e['sum_ttlnboftxsexctd'] = xbc9ca89e['ttlnboftxsexctd'].fillna(0) + xbc9ca89e['ttlnboftxsexctd_b'].fillna(0)
        xbc9ca89e['sum_del_qty'] = xbc9ca89e['qty_del'].fillna(0) + xbc9ca89e['qty_del_b'].fillna(0)
        xbc9ca89e['sum_delvry_trnovr'] = xbc9ca89e['delvry_trnovr'].fillna(0) + xbc9ca89e['delvry_trnovr_b'].fillna(0)
        xbc9ca89e['del_per'] = (xbc9ca89e['sum_del_qty'] / xbc9ca89e['sum_ttltradgvol'] * 100).round(2)
        xbc9ca89e['avg_price'] = (xbc9ca89e['sum_ttltrfval'] / xbc9ca89e['sum_ttltradgvol']).round(2)
        xbc9ca89e['avg_qty_per_order'] = (xbc9ca89e['sum_ttltradgvol'] / xbc9ca89e['sum_ttlnboftxsexctd']).round(2)
        xbc9ca89e['avg_order_price'] = (xbc9ca89e['avg_price'] * xbc9ca89e['avg_qty_per_order']).round(2)
        xbc9ca89e['close_to_avg'] = (xbc9ca89e['avg_price'] - xbc9ca89e['clspric']).round(2)
        xbc9ca89e['close_avg_perc'] = (xbc9ca89e['close_to_avg'] * 100 / xbc9ca89e['clspric']).round(2)
        xbc9ca89e = xbc9ca89e[['traddt', 'isin', 'fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'sctysrs', 'sctysrs_b', 'src', 'src_b', 'fininstrmnm', 'opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric', 'prvsclsgpric', 'sttlmpric', 'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del', 'delvry_trnovr', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b', 'sum_ttltradgvol', 'sum_ttltrfval', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'sum_delvry_trnovr', 'del_per', 'avg_price', 'avg_qty_per_order', 'avg_order_price', 'close_to_avg', 'close_avg_perc', 'othr_trds', 'othr_trds_vol', 'othr_trds_val', 'othr_trds_txsexctd']]
        sa_89a8394b = xbc9ca89e.copy().reset_index(drop=True)
    else:
        print('Date Mismatch in NSE BSE Combined data')
        print(f'Date:1-- {var_eeba8bb6} & Date:2-- {x367ef122}')
    return sa_89a8394b

def sa_bdc9a363(_4c51300a, var_cd8bc1aa, _334f7a3d, zx_eb105c7a):
    _a23a1d2f = xfa20b436(_4c51300a, var_cd8bc1aa)
    zx_0e3b8575 = var_f2e352ac(_334f7a3d, zx_eb105c7a)
    xbc9ca89e = pd.merge(_a23a1d2f, zx_0e3b8575, left_on='isin', right_on='isin_b', how='outer')
    xbc9ca89e['fininstrmid'] = xbc9ca89e['fininstrmid'].fillna(0).astype(int).astype(str)
    xbc9ca89e['fininstrmid'] = xbc9ca89e['fininstrmid'].replace('0', np.nan)
    sa_89a8394b = sa_62b57292(xbc9ca89e)
    return sa_89a8394b

def sa_e342d3f6():
    var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{var_4780fc62['user']}:{var_4780fc62['password']}@{var_4780fc62['host']}:{var_4780fc62['port']}/{var_4780fc62['dbname']}'
    engine = create_engine(conn_string)
    query = 'SELECT isin FROM my_static_data'
    sa_ae9a54ec = pd.read_sql_query(query, engine)
    var_479cc7da = sa_ae9a54ec[['isin']]
    var_69f08581 = set(var_479cc7da['isin'])
    return var_69f08581

def var_f8551387(sa_89a8394b):
    sa_37ad9730 = ['isin', 'fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'src', 'src_b', 'fininstrmnm']
    var_eaeb8393 = sa_89a8394b[sa_37ad9730]
    var_1d1dfb4f = var_eaeb8393[(var_eaeb8393['fininstrmid'].isnull() | (var_eaeb8393['fininstrmid'] == 0) | (var_eaeb8393['fininstrmid_b'] == '')) & (var_eaeb8393['fininstrmid_b'].isnull() | (var_eaeb8393['fininstrmid'] == 0) | (var_eaeb8393['fininstrmid_b'] == ''))]
    sa_be39f99e = var_eaeb8393[(var_eaeb8393['src'].isnull() | (var_eaeb8393['src'] == 0) | (var_eaeb8393['src_b'] == '')) & (var_eaeb8393['src_b'].isnull() | (var_eaeb8393['src'] == 0) | (var_eaeb8393['src_b'] == ''))]
    for index, row in var_1d1dfb4f.iterrows():
        print(f'Invalid FININSTRMID at ISIN: {row['isin']} - Both columns are empty or invalid.')
    for index, row in sa_be39f99e.iterrows():
        print(f'Invalid SRC at ISIN: {row['isin']} - Both columns are empty or invalid.')
    zx_c164102f = pd.concat([var_1d1dfb4f, sa_be39f99e])
    if not zx_c164102f.empty:
        x7c4a135d = pd.to_datetime(sa_89a8394b['traddt'][0]).strftime('%d-%m-%Y')
        print(f'Invalid rows found. saving to CSV file (invalid_rows-{x7c4a135d}) and Exiting...')
        zx_c164102f.to_csv(f'invalid_rows-{x7c4a135d}.csv', index=False)
        sys.exit()
    var_69f08581 = sa_e342d3f6()
    zx_f290001d = var_eaeb8393[~var_eaeb8393['isin'].isin(var_69f08581)]
    if not zx_f290001d.empty:
        print(f'----------Missing Data ------------')
        print(f'{zx_f290001d}')
    var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{var_4780fc62['user']}:{var_4780fc62['password']}@{var_4780fc62['host']}:{var_4780fc62['port']}/{var_4780fc62['dbname']}'
    engine = create_engine(conn_string)
    if not zx_f290001d.empty:
        zx_f290001d.to_sql('my_static_data', engine, if_exists='append', index=False)
        print('New static data inserted in Databse for Static Data.')
    else:
        print('No new data to insert in Databse for Static Data.')
    return zx_f290001d

def xe8c0a0a9(sa_89a8394b):
    _da88f560 = ['fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'src', 'src_b', 'fininstrmnm']
    xf33bad8d = sa_89a8394b.drop(columns=_da88f560)
    xf33bad8d['traddt'] = pd.to_datetime(xf33bad8d['traddt'])
    zx_d373b5c0 = {'isin': 20, 'sctysrs': 10, 'sctysrs_b': 10, 'othr_trds': 20}
    for col, sa_0f82aca6 in zx_d373b5c0.items():
        xf33bad8d[col] = xf33bad8d[col].astype(str).str.slice(0, sa_0f82aca6)
    x68d597bb = ['ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'othr_trds_vol', 'othr_trds_txsexctd']
    for col in x68d597bb:
        xf33bad8d[col] = pd.to_numeric(xf33bad8d[col], errors='coerce').astype('Int64')
    x68d597bb = ['ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'othr_trds_vol', 'othr_trds_txsexctd']
    for col in x68d597bb:
        xf33bad8d[col] = pd.to_numeric(xf33bad8d[col], errors='coerce').astype('Int64')
    var_4749223f = ['opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric', 'prvsclsgpric', 'sttlmpric', 'del_per', 'avg_price', 'avg_order_price', 'close_to_avg', 'close_avg_perc']
    for col in var_4749223f:
        xf33bad8d[col] = pd.to_numeric(xf33bad8d[col], errors='coerce').astype(float)
    var_4749223f = ['ttltrfval', 'delvry_trnovr', 'ttltrfval_b', 'delvry_trnovr_b', 'sum_ttltrfval', 'sum_delvry_trnovr', 'del_per', 'avg_price', 'avg_qty_per_order', 'avg_order_price', 'close_to_avg', 'close_avg_perc', 'othr_trds_val']
    for col in var_4749223f:
        xf33bad8d[col] = pd.to_numeric(xf33bad8d[col], errors='coerce').astype(float)
    var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{var_4780fc62['user']}:{var_4780fc62['password']}@{var_4780fc62['host']}:{var_4780fc62['port']}/{var_4780fc62['dbname']}'
    engine = create_engine(conn_string)
    try:
        xf33bad8d.to_sql('my_daily_data', engine, if_exists='append', index=False)
        print('Daily Data uploaded successfully.')
    except IntegrityError as e:
        print(f'Error occurred: {e.orig}')
    except Exception as e:
        print(f'An error occurred: {e}')
    finally:
        try:
            engine.dispose()
        except Exception as e:
            print(f'Error closing the connection: {e}')
    return xf33bad8d

def xfba190f0(sa_aaf3eb44, sa_65808d74, sa_400bb302):
    sa_400bb302 = str(sa_400bb302)
    sa_65808d74 = f'{sa_65808d74:02d}'
    for index, row in sa_aaf3eb44.iterrows():
        _acf3357e = row['Date_type']
        print(_acf3357e)
        sa_41834621 = _acf3357e.day
        sa_41834621 = f'{sa_41834621:02d}'
        _4c51300a, zx_eb105c7a, var_cd8bc1aa, _334f7a3d = zx_a679377a(sa_41834621, sa_65808d74, sa_400bb302)
        sa_89a8394b = sa_bdc9a363(_4c51300a, var_cd8bc1aa, _334f7a3d, zx_eb105c7a)
        zx_f290001d = var_f8551387(sa_89a8394b)
        xf33bad8d = xe8c0a0a9(sa_89a8394b)
    return

def zx_c6afc31b(zx_04bfce14, var_681b9f9f):
    zx_04bfce14 = datetime.strptime(zx_04bfce14, '%Y-%m-%d')
    var_681b9f9f = datetime.strptime(var_681b9f9f, '%Y-%m-%d')
    print(zx_04bfce14, var_681b9f9f)
    _e60e222a = zx_04bfce14.day
    var_0260dcc4 = zx_04bfce14.month
    var_4fb553ef = zx_04bfce14.year
    var_13c70891 = var_681b9f9f.day
    x375a90e6 = var_681b9f9f.month
    sa_6465cb6b = var_681b9f9f.year
    print(var_0260dcc4, x375a90e6, var_4fb553ef, sa_6465cb6b)
    if var_0260dcc4 != x375a90e6 or var_4fb553ef != sa_6465cb6b:
        zx_073c1634 = 'Use One Month only at a time'
        print(zx_073c1634)
    else:
        xe3c9de4d = sa_a64995fe(var_0260dcc4, var_4fb553ef)
        sa_aaf3eb44 = xe3c9de4d[(xe3c9de4d['Date_type'] >= zx_04bfce14) & (xe3c9de4d['Date_type'] <= var_681b9f9f)]
        xfba190f0(sa_aaf3eb44, var_0260dcc4, var_4fb553ef)
        zx_073c1634 = 'OK'
    return zx_073c1634