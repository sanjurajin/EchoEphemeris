
import json
import base64

import pandas as pd
import requests
import zipfile
import sys
import os
from datetime import datetime
import time
from sqlalchemy import create_engine
cwd = os.getcwd()
_bh5lj = os.path.join(cwd,'app','data','spec')
with open(os.path.join(_bh5lj,'c_end.json'), 'r') as f:
    eazhdj_jasf = f.read()
dh78ah = base64.b64decode(eazhdj_jasf).decode()
config = json.loads(dh78ah)
sym_col = config['columns']
import numpy as np
import psycopg2
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import IntegrityError
import hashlib
dfhkla_adhk = "3431f67b1c3a2caa558fd24fc37b02659abcccb614370bbb0cd9e93a64e4c0f1"
sdfsmj8972r = os.getlogin().encode() 
asjl_67as = hashlib.sha256(sdfsmj8972r).hexdigest()
if asjl_67as == dfhkla_adhk:
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
pt3 = f'/'+ 'BhavCopy_BSE_CM_0_0_0_'
def sa_49e00907():
    var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f"postgresql+psycopg2://{var_4780fc62[sym_col[70]]}:{var_4780fc62[sym_col[40]]}@{var_4780fc62[sym_col[29]]}:{var_4780fc62[sym_col[41]]}/{var_4780fc62[sym_col[17]]}"
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
    zx_f1016f82[sym_col[58]] = zx_f1016f82[sym_col[58]].fillna(zx_f1016f82[sym_col[59]])
    zx_f1016f82 = zx_f1016f82.sort_values(by=sym_col[25])
    zx_f1016f82 = zx_f1016f82.reset_index()
    sa_9e99a581 = zx_f1016f82[[sym_col[58], sym_col[25], sym_col[30]]]
    zx_ce535d10 = pd.DataFrame()
    zx_ce535d10[sym_col[57]] = sa_9e99a581[sym_col[58]] + ' :- ' + sa_9e99a581[sym_col[25]] + '  ' + sa_9e99a581[sym_col[30]]
    var_40025d04 = zx_ce535d10[sym_col[57]].tolist()
    return var_40025d04
loc2bh = 'mto/MTO_'

def zx_57414141(sa_b76a7ca1):
    try:
        zx_d887db09 = sa_b76a7ca1.split(':-')
        var_5b5f25e9 = zx_d887db09[0].strip()
        xfe80de81 = zx_d887db09[1].strip().rsplit(' ', 1)[0]
        var_7813612d = zx_d887db09[1].strip().rsplit(' ', 1)[1]
        var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
        conn_string = f"postgresql+psycopg2://{var_4780fc62[sym_col[70]]}:{var_4780fc62[sym_col[40]]}@{var_4780fc62[sym_col[29]]}:{var_4780fc62[sym_col[41]]}/{var_4780fc62[sym_col[17]]}"
        engine = create_engine(conn_string, connect_args={'connect_timeout': 10})
        query = '\n            SELECT * FROM my_daily_data\n            WHERE isin = %(isin)s\n        '
        with engine.connect().execution_options(timeout=30) as connection:
            xf33bad8d = pd.read_sql(query, connection, params={'isin': var_7813612d})
        if xf33bad8d.empty:
            print(f'No data found for ISIN: {var_7813612d}')
            return (pd.DataFrame(), 'None')
        xf33bad8d[sym_col[61]] = pd.to_datetime(xf33bad8d[sym_col[61]], dayfirst=True)
        xf33bad8d[sym_col[60]] = xf33bad8d[sym_col[61]]
        xf33bad8d = xf33bad8d.sort_values(by=sym_col[60])
        xf33bad8d = xf33bad8d.drop(columns=[sym_col[61]])
        var_6c4662bd = xf33bad8d[sym_col[53]].sum()
        zx_42e21f40 = xf33bad8d[sym_col[56]].sum()
        _87651205 = var_6c4662bd * 100 / zx_42e21f40
        xf33bad8d[sym_col[6]] = _87651205.round(4)
        _67b2649f = xf33bad8d[sym_col[8]].mean().round(2)
        xf33bad8d[sym_col[8]] = (xf33bad8d[sym_col[8]] / 1000).round(3)
        xf33bad8d[sym_col[7]] = _67b2649f
        xf33bad8d[sym_col[7]] = (xf33bad8d[sym_col[7]] / 1000).round(3)
        xf33bad8d[sym_col[53]] = (xf33bad8d[sym_col[53]] / 1000000).round(3)
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
loc1bh = "BhavCopy_NSE_CM_0_0_0_"
def var_a2cc7414():
    var_d5d49ccf = os.path.join(cwd, 'app', 'data', 'exch_files', 'calander_file.csv')
    _9f481db4 = pd.read_csv(var_d5d49ccf)
    zx_2f6adb17 = _9f481db4[-1:].values[0]
    zx_2f6adb17 = pd.to_datetime(zx_2f6adb17[0], format='%d-%b-%y').strftime('%d-%m-%Y')
    return zx_2f6adb17

def _sajc8dhks():
    with open(os.path.join(_bh5lj,'c_ur.json'), 'r') as f:
        config = json.load(f)
    encoded_loc1 = config['encoded_loc1']
    encoded_loc2 = config['encoded_loc2']
    encoded_olb = config['encoded_olb']
    encoded_web = config['encoded_web']
    encoded_loc3 = config['encoded_loc3']
    encoded_olz = config['encoded_olz']
    encoded_hjxo = config['encoded_hjxo']
    loc1 = base64.b64decode(encoded_loc1).decode()
    loc2 = base64.b64decode(encoded_loc2).decode()
    olb = base64.b64decode(encoded_olb).decode()
    web = base64.b64decode(encoded_web).decode()
    loc3 = base64.b64decode(encoded_loc3).decode()
    olz = base64.b64decode(encoded_olz).decode()
    hjxo = base64.b64decode(encoded_hjxo).decode()

    return loc1, loc2, olb, web,  loc3, olz, hjxo

def xe4d70624(x0987903c):
    sa_67392fd3 = pd.DataFrame(x0987903c, columns=[sym_col[2]])
    sa_67392fd3[sym_col[2]] = pd.to_datetime(sa_67392fd3[sym_col[2]], format='%d-%m-%Y', dayfirst=True).dt.strftime('%d-%b-%y')
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
        xe3c9de4d[sym_col[3]] = pd.to_datetime(xe3c9de4d[sym_col[3]])
        xe3c9de4d = xe3c9de4d[(xe3c9de4d[sym_col[3]] >= var_ee01b9df) & (xe3c9de4d[sym_col[3]] <= sa_24938c07)]
        if not xe3c9de4d.empty:
            for index, row in xe3c9de4d.iterrows():
                _acf3357e = row[sym_col[3]]
                x995b5cdf = row[sym_col[11]]
                _ab710224 = row[sym_col[19]]
                var_469f4308 = row[sym_col[37]]
                sa_cb705957 = row[sym_col[71]]
                zx_f96a6410 = row[sym_col[22]]
                print('--------------------------')
                print(row[sym_col[2]])
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
    _9f481db4[sym_col[11]] = pd.to_datetime(_9f481db4[sym_col[2]], format='%d-%b-%y').dt.strftime('%Y%m%d')
    _9f481db4[sym_col[19]] = pd.to_datetime(_9f481db4[sym_col[2]], format='%d-%b-%y').dt.strftime('%d%m%Y')
    _9f481db4[sym_col[3]] = pd.to_datetime(_9f481db4[sym_col[2]], format='%d-%b-%y')
    _9f481db4[sym_col[37]] = pd.to_datetime(_9f481db4[sym_col[2]], format='%d-%b-%y').dt.strftime('%d%m%y')
    _9f481db4[sym_col[71]] = pd.to_datetime(_9f481db4[sym_col[2]], format='%d-%b-%y').dt.year
    _9f481db4[sym_col[16]] = pd.to_datetime(_9f481db4[sym_col[2]], format='%d-%b-%y').dt.day
    _9f481db4[sym_col[36]] = pd.to_datetime(_9f481db4[sym_col[2]], format='%d-%b-%y').dt.month
    _9f481db4[sym_col[22]] = _9f481db4[sym_col[16]].astype(str).str.zfill(2) + _9f481db4[sym_col[36]].astype(str).str.zfill(2)
    x30ba289b = _9f481db4[(_9f481db4[sym_col[3]].dt.month == sa_65808d74) & (_9f481db4[sym_col[3]].dt.year == sa_400bb302)]
    return x30ba289b

def _372ff396(x995b5cdf, _ab710224):
    loc1, loc2, olb, web,  loc3, olz, hjxo =_sajc8dhks()
    loc1 = f'{loc1}/{loc1bh}'
    xc1c8d369 = f'{loc1}{x995b5cdf}_F_0000.csv.zip'
    loc2 =f"{loc2}/{loc2bh}"
    sa_2d3a708a = f'{loc2}{_ab710224}.DAT'
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
        
        payload = pd.read_csv(f'{olb}' + _ab710224 + '.csv')
        if not payload.empty:
            payload.columns = payload.columns.str.strip()
            payload[sym_col[69]] = payload[sym_col[0]] * payload[sym_col[4]]
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
    loc1, loc2, olb, web,  loc3, olz, hjxo =_sajc8dhks()
    var_80f67872 = f'{loc3}{pt3}{x995b5cdf}_F_0000.CSV'
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    response = requests.get(var_80f67872, headers=headers)
    zip_file_path = os.path.join(sa_6e998b78)
    file = os.path.join(sa_6e998b78, 'BSE Files', f'BhavCopy_BSE_CM_0_0_0_{x995b5cdf}_F_0000.csv')
    if response.status_code == 200:
        with open(file, 'wb') as file:
            file.write(response.content)
        print('Bhavcopy_BSE completed successfully!')
    else:
        _3ba85c2f = f'{olz}{var_469f4308}.zip'
        response = requests.get(_3ba85c2f, headers=headers)
        if response.status_code == 200:
            file = os.path.join(sa_6e998b78, 'BSE Files', f'BhavCopy_BSE_CM_0_0_0_{x995b5cdf}_F_0000_O.csv')
            with open(file, 'wb') as file:
                file.write(response.content)
            print('Bhavcopy_BSE Old completed successfully!')
        else:
            print(f'Failed to download BSE Bhavcopy file')
    axbhs2 = 'SCBSEALL'
    _ec2f02ea = f'{hjxo}/{sa_cb705957}/{axbhs2}{zx_f96a6410}.zip'
    
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
        var_78c8768c = var_4be6b81c[sym_col[5]][0]
    elif 'DATE1' in var_4be6b81c.columns:
        var_78c8768c = var_4be6b81c[sym_col[1]][0].strip()
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
        _a23a1d2f = pd.merge(var_4be6b81c, x74e9446e, left_on=sym_col[58], right_on='security', how='left')
        _fe9f5edb = ['traddt', 'fininstrmid', 'fininstrmnm', 'isin', 'src', 'tckrsymb', 'sctysrs', 'opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric', 'prvsclsgpric', 'sttlmpric', 'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del']
        _a23a1d2f = _a23a1d2f[_fe9f5edb]
        _a23a1d2f[sym_col[20]] = (_a23a1d2f[sym_col[67]] / _a23a1d2f[sym_col[65]] * _a23a1d2f[sym_col[44]]).round(2)
        _a23a1d2f = _a23a1d2f.groupby([sym_col[30], sym_col[46]], as_index=False).agg({'qty_del': 'sum', 'delvry_trnovr': 'sum', 'src': 'first', 'traddt': 'first', 'isin': 'first', 'fininstrmid': 'first', 'tckrsymb': 'first', 'sctysrs': 'first', 'fininstrmnm': 'first', 'opnpric': 'first', 'hghpric': 'first', 'lwpric': 'first', 'clspric': 'first', 'lastpric': 'first', 'prvsclsgpric': 'first', 'sttlmpric': 'first', 'ttltradgvol': 'first', 'ttltrfval': 'first', 'ttlnboftxsexctd': 'first'})
        zx_c7c9c9b8 = _a23a1d2f.columns.tolist()
        zx_912072f1 = [col for col in zx_c7c9c9b8 if col not in ['qty_del', 'delvry_trnovr']] + ['qty_del', 'delvry_trnovr']
        _a23a1d2f = _a23a1d2f[zx_912072f1]
        _a23a1d2f[sym_col[46]] = _a23a1d2f[sym_col[46]].fillna('N/A')
        sa_5c10987b = ['GS', 'GB', 'TB', 'SG', 'N0', 'N1', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9', 'E1', 'X1', 'Y0', 'Y1', 'Y2', 'Y3', 'Y4', 'Y5', 'Y6', 'Y7', 'Y8', 'YA', 'YC', 'YG', 'YI', 'YK', 'YL', 'YM', 'YN', 'YO', 'YP', 'YQ', 'YR', 'YS', 'YT', 'YV', 'YW', 'YX', 'YY', 'YZ', 'ZY', 'Z0', 'Z2', 'Z3', 'Z4', 'Z5', 'Z6', 'Z7', 'Z8', 'Z9', 'ZB', '    ZC', 'ZD', 'ZE', 'ZF', 'ZG', 'ZH', 'ZI', 'ZJ', 'ZK', 'ZL', 'ZM', 'ZN', 'ZP', 'ZR', 'ZS', 'ZT', 'ZU', 'ZW', 'ZX', 'NB', 'NC', 'ND', 'NE', 'NF', 'NG', 'NH', 'NI', 'NJ', 'NK', 'NL', 'NM', 'NN', 'NO', 'NP', 'NQ', 'NR', 'NS', 'NT', 'NU', 'NV', 'NW', 'NX', 'NY', 'NZ', 'AB', 'AC', 'AG', 'AH', 'AI', 'AJ', 'AL', 'AN', 'AO', 'AP', 'AR', 'AT', 'AV', 'AW', 'AX', 'AY', 'AZ']
        _a23a1d2f = _a23a1d2f[~_a23a1d2f[sym_col[46]].isin(sa_5c10987b)]
        _a23a1d2f.reset_index(drop=True, inplace=True)
        x596d40a4 = _a23a1d2f[sym_col[46]].isin(['BE', 'BZ']) & (_a23a1d2f[sym_col[44]] == 0)
        _a23a1d2f.loc[x596d40a4, 'qty_del'] = _a23a1d2f[sym_col[65]]
        _a23a1d2f.loc[x596d40a4, 'delvry_trnovr'] = _a23a1d2f[sym_col[67]]
        zx_7d2f4792 = ['BE', 'BZ', 'EQ', 'SM', 'ST', 'MF', 'ME']
        x558bae4c = _a23a1d2f[~_a23a1d2f[sym_col[46]].isin(zx_7d2f4792)]
        x558bae4c.reset_index(drop=True, inplace=True)
        var_bc16acc0 = _a23a1d2f[_a23a1d2f[sym_col[46]].isin(zx_7d2f4792)]
        var_bc16acc0.reset_index(drop=True, inplace=True)
        x558bae4c = x558bae4c.groupby(sym_col[58], as_index=False).agg(ttltradgvol=('ttltradgvol', 'sum'), ttltrfval=('ttltrfval', 'sum'), ttlnboftxsexctd=('ttlnboftxsexctd', 'sum'), othr_trds=('sctysrs', lambda x: ', '.join(x)))
        xed256730 = var_bc16acc0.copy()
        xed256730 = var_bc16acc0.merge(x558bae4c, on=sym_col[58], how='left', suffixes=('', '_y'))
        xed256730.rename(columns={'othr_trds_y': 'othr_trds', 'ttltradgvol_y': 'othr_trds_vol', 'ttltrfval_y': 'othr_trds_val', 'ttlnboftxsexctd_y': 'othr_trds_txsexctd'}, inplace=True)
        _a23a1d2f = xed256730.copy()
        sa_0abf75cb = _a23a1d2f[sym_col[58]].value_counts()
        xa5d9de3b = sa_0abf75cb[sa_0abf75cb > 1]
        if len(xa5d9de3b) != 0:
            for zx_2c6bc158 in xa5d9de3b.index:
                _5282482f = _a23a1d2f[_a23a1d2f[sym_col[58]] == zx_2c6bc158]
                print(_5282482f[['traddt', 'isin', 'fininstrmid', 'tckrsymb', 'sctysrs', 'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del', 'delvry_trnovr']])
                print('Merged_df_nse have more than 1 entry for above Symbols')
                print(zx_2c6bc158)
                if _5282482f[sym_col[44]].nunique() == 1:
                    x0860132f = input('Do you want to remove one of these entries? (Enter or yes/no): ')
                    if x0860132f.lower() == 'yes' or x0860132f == '':
                        _a23a1d2f = _a23a1d2f[~((_a23a1d2f[sym_col[58]] == zx_2c6bc158) & (_a23a1d2f[sym_col[46]] != 'EQ'))]
                        print(f'Removed one entry for {zx_2c6bc158}.')
                    elif x0860132f.lower() == 'no':
                        print(f'Different qty_del for {zx_2c6bc158}: {_5282482f[sym_col[44]].unique()}')
                    else:
                        print('Invalid input.')
                        _a23a1d2f = _a23a1d2f[~((_a23a1d2f[sym_col[58]] == zx_2c6bc158) & (_a23a1d2f[sym_col[46]] != 'EQ'))]
                        print(f'Removed one entry for {zx_2c6bc158}.')
        x00f317ee = _a23a1d2f[sym_col[30]].value_counts()
        sa_d4c4dcfd = x00f317ee[x00f317ee > 1]
        if len(sa_d4c4dcfd) != 0:
            for x48a4d578 in sa_d4c4dcfd.index:
                zx_7f6c6b1f = _a23a1d2f[_a23a1d2f[sym_col[30]] == x48a4d578]
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
    var_463a048b = x497f1234[sym_col[62]][0]
    zx_3e2a8280 = zx_d5906e21[sym_col[16]][0]
    _db8b2175 = str(zx_3e2a8280)
    zx_32e1b76e = datetime.strptime(_db8b2175, '%d%m%Y').strftime('%Y-%m-%d')
    if zx_32e1b76e == var_463a048b:
        var_b027a25f = 'True'
    else:
        print('The dates are not same for BSE Files.')
        var_b027a25f = 'Flase'
    if var_b027a25f == 'True':
        x497f1234[sym_col[24]] = x497f1234[sym_col[24]].astype(str)
        zx_0e3b8575 = pd.merge(x497f1234, zx_d5906e21, left_on=sym_col[26], right_on='scrip_code', how='left')
        xe5400f53 = ['traddt_b', 'src_b', 'fininstrmid_b', 'isin_b', 'fininstrmnm_b', 'tckrsymb_b', 'sctysrs_b', 'opnpric_b', 'hghpric_b', 'lwpric_b', 'clspric_b', 'lastpric_b', 'prvsclsgpric_b', 'sttlmpric_b', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b', 'day_volume_b', 'day_trnover_b', 'day_perc_b']
        zx_0e3b8575 = zx_0e3b8575[xe5400f53]
        zx_0e3b8575[sym_col[47]] = zx_0e3b8575[sym_col[47]].fillna('N/A')
        sa_0abf75cb = zx_0e3b8575[sym_col[59]].value_counts()
        x6e8f3834 = sa_0abf75cb[sa_0abf75cb > 1]
        if len(x6e8f3834) != 0:
            for zx_2c6bc158 in x6e8f3834.index:
                _5282482f = zx_0e3b8575[zx_0e3b8575[sym_col[59]] == zx_2c6bc158]
                print(_5282482f[['traddt_b', 'isin_b', 'fininstrmid_b', 'tckrsymb_b', 'sctysrs_b', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b']])
                print('Merged_df_bse have more than 1 entry for above Symbols')
                print(zx_2c6bc158)
                if _5282482f[sym_col[45]].nunique() == 1:
                    x0860132f = input('Do you want to remove one of these entries? (yes/no): ')
                    if x0860132f.lower() == 'yes':
                        zx_0e3b8575 = zx_0e3b8575[~((zx_0e3b8575[sym_col[59]] == zx_2c6bc158) & (zx_0e3b8575[sym_col[47]] != 'EQ'))]
                        print(f'Removed one entry for {zx_2c6bc158}.')
                else:
                    print(f'Different qty_del for {zx_2c6bc158}: {_5282482f[sym_col[45]].unique()}')
        x00f317ee = zx_0e3b8575[sym_col[31]].value_counts()
        sa_d4c4dcfd = x00f317ee[x00f317ee > 1]
        if len(sa_d4c4dcfd) != 0:
            for x48a4d578 in sa_d4c4dcfd.index:
                _5282482f = zx_0e3b8575[zx_0e3b8575[sym_col[31]] == x48a4d578]
                print(_5282482f[['traddt_b', 'isin_b', 'fininstrmid_b', 'tckrsymb_b', 'sctysrs_b', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b']])
                print('Merged_df_bse have more than 1 entry for above ISIN')
                print(x48a4d578)
                if _5282482f[sym_col[45]].nunique() == 1:
                    x0860132f = input('Do you want to remove one of these entries? (yes/no): ')
                    if x0860132f.lower() == 'yes':
                        zx_0e3b8575 = zx_0e3b8575[~((zx_0e3b8575[sym_col[31]] == x48a4d578) & zx_0e3b8575[sym_col[45]].isna())]
                        print(f'Removed one entry for {x48a4d578} where Delivery Quantity is NaN.')
                else:
                    print(f'Different qty_del for {zx_2c6bc158}: {_5282482f[sym_col[45]].unique()}')
        sa_5c10987b = ['G', 'F']
        zx_0e3b8575 = zx_0e3b8575[~zx_0e3b8575[sym_col[47]].isin(sa_5c10987b)]
        zx_0e3b8575.reset_index(drop=True, inplace=True)
        return zx_0e3b8575
    else:
        print('Dates Mismatch for BSE Files')
        print('Check Files')
        print(f'Bhavcopy BSE {_334f7a3d}')
        print(f'Delivery BSE {zx_eb105c7a}')

def sa_62b57292(xbc9ca89e):
    var_eeba8bb6 = xbc9ca89e[sym_col[61]].unique()[0]
    x367ef122 = xbc9ca89e[sym_col[62]].unique()[0]
    if var_eeba8bb6 == x367ef122 or var_eeba8bb6 != x367ef122:
        xbc9ca89e[sym_col[61]] = xbc9ca89e[sym_col[61]].fillna(xbc9ca89e[sym_col[62]])
        xbc9ca89e[sym_col[30]] = xbc9ca89e[sym_col[30]].fillna(xbc9ca89e[sym_col[31]])
        xbc9ca89e[sym_col[38]] = xbc9ca89e[sym_col[38]].fillna(xbc9ca89e[sym_col[39]])
        xbc9ca89e[sym_col[27]] = xbc9ca89e[sym_col[27]].fillna(xbc9ca89e[sym_col[28]])
        xbc9ca89e[sym_col[34]] = xbc9ca89e[sym_col[34]].fillna(xbc9ca89e[sym_col[35]])
        xbc9ca89e[sym_col[14]] = xbc9ca89e[sym_col[14]].fillna(xbc9ca89e[sym_col[15]])
        xbc9ca89e[sym_col[42]] = xbc9ca89e[sym_col[42]].fillna(xbc9ca89e[sym_col[43]])
        xbc9ca89e[sym_col[50]] = xbc9ca89e[sym_col[50]].fillna(xbc9ca89e[sym_col[51]])
        xbc9ca89e[sym_col[32]] = xbc9ca89e[sym_col[32]].fillna(xbc9ca89e[sym_col[33]])
        xbc9ca89e[sym_col[25]] = xbc9ca89e[sym_col[25]].fillna(xbc9ca89e[sym_col[26]])
        xbc9ca89e[sym_col[55]] = xbc9ca89e[sym_col[65]].fillna(0) + xbc9ca89e[sym_col[66]].fillna(0)
        xbc9ca89e[sym_col[56]] = xbc9ca89e[sym_col[67]].fillna(0) + xbc9ca89e[sym_col[68]].fillna(0)
        xbc9ca89e[sym_col[54]] = xbc9ca89e[sym_col[63]].fillna(0) + xbc9ca89e[sym_col[64]].fillna(0)
        xbc9ca89e[sym_col[52]] = xbc9ca89e[sym_col[44]].fillna(0) + xbc9ca89e[sym_col[45]].fillna(0)
        xbc9ca89e[sym_col[53]] = xbc9ca89e[sym_col[20]].fillna(0) + xbc9ca89e[sym_col[21]].fillna(0)
        xbc9ca89e[sym_col[18]] = (xbc9ca89e[sym_col[52]] / xbc9ca89e[sym_col[55]] * 100).round(2)
        xbc9ca89e[sym_col[9]] = (xbc9ca89e[sym_col[56]] / xbc9ca89e[sym_col[55]]).round(2)
        xbc9ca89e[sym_col[10]] = (xbc9ca89e[sym_col[55]] / xbc9ca89e[sym_col[54]]).round(2)
        xbc9ca89e[sym_col[8]] = (xbc9ca89e[sym_col[9]] * xbc9ca89e[sym_col[10]]).round(2)
        xbc9ca89e[sym_col[13]] = (xbc9ca89e[sym_col[9]] - xbc9ca89e[sym_col[14]]).round(2)
        xbc9ca89e[sym_col[12]] = (xbc9ca89e[sym_col[13]] * 100 / xbc9ca89e[sym_col[14]]).round(2)
        xbc9ca89e = xbc9ca89e[['traddt', 'isin', 'fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'sctysrs', 'sctysrs_b', 'src', 'src_b', 'fininstrmnm', 'opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric', 'prvsclsgpric', 'sttlmpric', 'ttltradgvol', 'ttltrfval', 'ttlnboftxsexctd', 'qty_del', 'delvry_trnovr', 'ttltradgvol_b', 'ttltrfval_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'delvry_trnovr_b', 'sum_ttltradgvol', 'sum_ttltrfval', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'sum_delvry_trnovr', 'del_per', 'avg_price', 'avg_qty_per_order', 'avg_order_price', 'close_to_avg', 'close_avg_perc', 'othr_trds', 'othr_trds_vol', 'othr_trds_val', 'othr_trds_txsexctd']]
        sa_89a8394b = xbc9ca89e.copy().reset_index(drop=True)
    else:
        print('Date Mismatch in NSE BSE Combined data')
        print(f'Date:1-- {var_eeba8bb6} & Date:2-- {x367ef122}')
    return sa_89a8394b

def sa_bdc9a363(_4c51300a, var_cd8bc1aa, _334f7a3d, zx_eb105c7a):
    _a23a1d2f = xfa20b436(_4c51300a, var_cd8bc1aa)
    zx_0e3b8575 = var_f2e352ac(_334f7a3d, zx_eb105c7a)
    xbc9ca89e = pd.merge(_a23a1d2f, zx_0e3b8575, left_on=sym_col[30], right_on=sym_col[31], how='outer')
    xbc9ca89e[sym_col[23]] = xbc9ca89e[sym_col[23]].fillna(0).astype(int).astype(str)
    xbc9ca89e[sym_col[23]] = xbc9ca89e[sym_col[23]].replace('0', np.nan)
    sa_89a8394b = sa_62b57292(xbc9ca89e)
    return sa_89a8394b

def sa_e342d3f6():
    var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f"postgresql+psycopg2://{var_4780fc62[sym_col[70]]}:{var_4780fc62[sym_col[40]]}@{var_4780fc62[sym_col[29]]}:{var_4780fc62[sym_col[41]]}/{var_4780fc62[sym_col[17]]}"
    engine = create_engine(conn_string)
    query = 'SELECT isin FROM my_static_data'
    sa_ae9a54ec = pd.read_sql_query(query, engine)
    var_479cc7da = sa_ae9a54ec[[sym_col[30]]]
    var_69f08581 = set(var_479cc7da[sym_col[30]])
    return var_69f08581

def var_f8551387(sa_89a8394b):
    sa_37ad9730 = ['isin', 'fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'src', 'src_b', 'fininstrmnm']
    var_eaeb8393 = sa_89a8394b[sa_37ad9730]
    var_1d1dfb4f = var_eaeb8393[(var_eaeb8393[sym_col[23]].isnull() | (var_eaeb8393[sym_col[23]] == 0) | (var_eaeb8393[sym_col[24]] == '')) & (var_eaeb8393[sym_col[24]].isnull() | (var_eaeb8393[sym_col[23]] == 0) | (var_eaeb8393[sym_col[24]] == ''))]
    sa_be39f99e = var_eaeb8393[(var_eaeb8393[sym_col[48]].isnull() | (var_eaeb8393[sym_col[48]] == 0) | (var_eaeb8393[sym_col[49]] == '')) & (var_eaeb8393[sym_col[49]].isnull() | (var_eaeb8393[sym_col[48]] == 0) | (var_eaeb8393[sym_col[49]] == ''))]
    for index, row in var_1d1dfb4f.iterrows():
        print(f'Invalid FININSTRMID at ISIN: {row[sym_col[30]]} - Both columns are empty or invalid.')
    for index, row in sa_be39f99e.iterrows():
        print(f'Invalid SRC at ISIN: {row[sym_col[30]]} - Both columns are empty or invalid.')
    zx_c164102f = pd.concat([var_1d1dfb4f, sa_be39f99e])
    if not zx_c164102f.empty:
        x7c4a135d = pd.to_datetime(sa_89a8394b[sym_col[61]][0]).strftime('%d-%m-%Y')
        print(f'Invalid rows found. saving to CSV file (invalid_rows-{x7c4a135d}) and Exiting...')
        zx_c164102f.to_csv(f'invalid_rows-{x7c4a135d}.csv', index=False)
        sys.exit()
    var_69f08581 = sa_e342d3f6()
    zx_f290001d = var_eaeb8393[~var_eaeb8393[sym_col[30]].isin(var_69f08581)]
    var_4780fc62 = {'dbname': _b4c16391, 'user': sa_a0e71f8f, 'password': _2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f"postgresql+psycopg2://{var_4780fc62[sym_col[70]]}:{var_4780fc62[sym_col[40]]}@{var_4780fc62[sym_col[29]]}:{var_4780fc62[sym_col[41]]}/{var_4780fc62[sym_col[17]]}"
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
    xf33bad8d[sym_col[61]] = pd.to_datetime(xf33bad8d[sym_col[61]])
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
    conn_string = f"postgresql+psycopg2://{var_4780fc62[sym_col[70]]}:{var_4780fc62[sym_col[40]]}@{var_4780fc62[sym_col[29]]}:{var_4780fc62[sym_col[41]]}/{var_4780fc62[sym_col[17]]}"
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
        _acf3357e = row[sym_col[3]]
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
        sa_aaf3eb44 = xe3c9de4d[(xe3c9de4d[sym_col[3]] >= zx_04bfce14) & (xe3c9de4d[sym_col[3]] <= var_681b9f9f)]
        xfba190f0(sa_aaf3eb44, var_0260dcc4, var_4fb553ef)
        zx_073c1634 = 'OK'
    return zx_073c1634