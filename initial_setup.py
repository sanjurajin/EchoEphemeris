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
import hashlib

dfhkla_adhk = "3431f67b1c3a2caa558fd24fc37b02659abcccb614370bbb0cd9e93a64e4c0f1"
sdfsmj8972r = os.getlogin().encode() 
asjl_67as = hashlib.sha256(sdfsmj8972r).hexdigest()
if asjl_67as == dfhkla_adhk:
    sa_33b379a7 = os.path.join(cwd, 'personal_data', 'creden.txt')
    var_631aada4 = open(sa_33b379a7, 'r').read().split()
    var_b4c16391 = var_631aada4[0]
    zx_a0e71f8f = var_631aada4[1]
    var_2e48e91c = var_631aada4[2]
else:
    sa_33b379a7 = os.path.join(cwd, 'app', 'creden.txt')
    var_631aada4 = open(sa_33b379a7, 'r').read().split()
    var_b4c16391 = var_631aada4[0]
    zx_a0e71f8f = var_631aada4[1]
    var_2e48e91c = var_631aada4[2]
var_262e59d9 = os.path.join(cwd, 'app', 'data', 'exch_files', 'initail_setup.csv')
sa_89a8394b = pd.read_csv(var_262e59d9)
 
def sa_e342d3f6():
    zx_4780fc62 = {'dbname': var_b4c16391, 'user': zx_a0e71f8f, 'password': var_2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f"postgresql+psycopg2://{zx_4780fc62['user']}:{zx_4780fc62['password']}@{zx_4780fc62['host']}:{zx_4780fc62['port']}/{zx_4780fc62['dbname']}"
    engine = create_engine(conn_string)
    query = 'SELECT isin FROM my_static_data'
    zx_ae9a54ec = pd.read_sql_query(query, engine)
    var_479cc7da = zx_ae9a54ec[['isin']]
    zx_69f08581 = set(var_479cc7da['isin'])
    return zx_69f08581

def sa_f8551387(sa_89a8394b):
    x37ad9730 = ['isin', 'fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'src', 'src_b', 'fininstrmnm']
    sa_eaeb8393 = sa_89a8394b[x37ad9730]
    var_1d1dfb4f = sa_eaeb8393[(sa_eaeb8393['fininstrmid'].isnull() | (sa_eaeb8393['fininstrmid'] == 0) | (sa_eaeb8393['fininstrmid_b'] == '')) & (sa_eaeb8393['fininstrmid_b'].isnull() | (sa_eaeb8393['fininstrmid'] == 0) | (sa_eaeb8393['fininstrmid_b'] == ''))]
    _be39f99e = sa_eaeb8393[(sa_eaeb8393['src'].isnull() | (sa_eaeb8393['src'] == 0) | (sa_eaeb8393['src_b'] == '')) & (sa_eaeb8393['src_b'].isnull() | (sa_eaeb8393['src'] == 0) | (sa_eaeb8393['src_b'] == ''))]
    for index, row in var_1d1dfb4f.iterrows():
        print(f'Invalid FININSTRMID at ISIN: {row['isin']} - Both columns are empty or invalid.')
    for index, row in _be39f99e.iterrows():
        print(f'Invalid SRC at ISIN: {row['isin']} - Both columns are empty or invalid.')
    sa_c164102f = pd.concat([var_1d1dfb4f, _be39f99e])
    if not sa_c164102f.empty:
        sa_7c4a135d = pd.to_datetime(sa_89a8394b['traddt'][0]).strftime('%d-%m-%Y')
        print(f'Invalid rows found. saving to CSV file (invalid_rows-{sa_7c4a135d}) and Exiting...')
        sa_c164102f.to_csv(f'invalid_rows-{sa_7c4a135d}.csv', index=False)
        sys.exit()
    zx_69f08581 = sa_e342d3f6()
    zx_f290001d = sa_eaeb8393[~sa_eaeb8393['isin'].isin(zx_69f08581)]
    # if not zx_f290001d.empty:
    #     # print(f'----------Missing Data ------------')
    #     # print(f'{zx_f290001d}')
    zx_4780fc62 = {'dbname': var_b4c16391, 'user': zx_a0e71f8f, 'password': var_2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f"postgresql+psycopg2://{zx_4780fc62['user']}:{zx_4780fc62['password']}@{zx_4780fc62['host']}:{zx_4780fc62['port']}/{zx_4780fc62['dbname']}"
    engine = create_engine(conn_string)
    if not zx_f290001d.empty:
        zx_f290001d.to_sql('my_static_data', engine, if_exists='append', index=False)
        print('New static data inserted in Databse for Static Data.')
    else:
        print('No new data to insert in Databse for Static Data.')
    return zx_f290001d

def xe8c0a0a9(sa_89a8394b):
    sa_da88f560 = ['fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'src', 'src_b', 'fininstrmnm', 'Unnamed: 0']
    zx_f33bad8d = sa_89a8394b.drop(columns=sa_da88f560)
    zx_f33bad8d['traddt'] = pd.to_datetime(zx_f33bad8d['traddt'])
    sa_d373b5c0 = {'isin': 20, 'sctysrs': 10, 'sctysrs_b': 10, 'othr_trds': 20}
    for col, _0f82aca6 in sa_d373b5c0.items():
        zx_f33bad8d[col] = zx_f33bad8d[col].astype(str).str.slice(0, _0f82aca6)
    _68d597bb = ['ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'othr_trds_vol', 'othr_trds_txsexctd']
    for col in _68d597bb:
        zx_f33bad8d[col] = pd.to_numeric(zx_f33bad8d[col], errors='coerce').astype('Int64')
    _68d597bb = ['ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'othr_trds_vol', 'othr_trds_txsexctd']
    for col in _68d597bb:
        zx_f33bad8d[col] = pd.to_numeric(zx_f33bad8d[col], errors='coerce').astype('Int64')
    sa_4749223f = ['opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric', 'prvsclsgpric', 'sttlmpric', 'del_per', 'avg_price', 'avg_order_price', 'close_to_avg', 'close_avg_perc']
    for col in sa_4749223f:
        zx_f33bad8d[col] = pd.to_numeric(zx_f33bad8d[col], errors='coerce').astype(float)
    sa_4749223f = ['ttltrfval', 'delvry_trnovr', 'ttltrfval_b', 'delvry_trnovr_b', 'sum_ttltrfval', 'sum_delvry_trnovr', 'del_per', 'avg_price', 'avg_qty_per_order', 'avg_order_price', 'close_to_avg', 'close_avg_perc', 'othr_trds_val']
    for col in sa_4749223f:
        zx_f33bad8d[col] = pd.to_numeric(zx_f33bad8d[col], errors='coerce').astype(float)
    zx_4780fc62 = {'dbname': var_b4c16391, 'user': zx_a0e71f8f, 'password': var_2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f"postgresql+psycopg2://{zx_4780fc62['user']}:{zx_4780fc62['password']}@{zx_4780fc62['host']}:{zx_4780fc62['port']}/{zx_4780fc62['dbname']}"
    engine = create_engine(conn_string)
    try:
        zx_f33bad8d.to_sql('my_daily_data', engine, if_exists='append', index=False)
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
    return zx_f33bad8d

def _7c56ebe4(sa_89a8394b):
    sa_f8551387(sa_89a8394b)
    xe8c0a0a9(sa_89a8394b)
_7c56ebe4(sa_89a8394b)