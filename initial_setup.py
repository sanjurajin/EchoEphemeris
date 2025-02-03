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
x33b379a7 = os.path.join(cwd, 'app', 'creden.txt')
var_631aada4 = open(x33b379a7, 'r').read().split()
xb4c16391 = var_631aada4[0]
var_a0e71f8f = var_631aada4[1]
zx_2e48e91c = var_631aada4[2]
sA_262e59d9 = os.path.join(cwd, 'app', 'data', 'exch_files', 'initail_setup.csv')
zx_89a8394b = pd.read_csv(sA_262e59d9)

def xe342d3f6():
    zx_4780fc62 = {'dbname': xb4c16391, 'user': var_a0e71f8f, 'password': zx_2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{zx_4780fc62['user']}:{zx_4780fc62['password']}@{zx_4780fc62['host']}:{zx_4780fc62['port']}/{zx_4780fc62['dbname']}'
    engine = create_engine(conn_string)
    query = 'SELECT isin FROM my_static_data'
    sA_ae9a54ec = pd.read_sql_query(query, engine)
    x479cc7da = sA_ae9a54ec[['isin']]
    x69f08581 = set(x479cc7da['isin'])
    return x69f08581

def _f8551387(zx_89a8394b):
    var_37ad9730 = ['isin', 'fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'src', 'src_b', 'fininstrmnm']
    var_eaeb8393 = zx_89a8394b[var_37ad9730]
    zx_1d1dfb4f = var_eaeb8393[(var_eaeb8393['fininstrmid'].isnull() | (var_eaeb8393['fininstrmid'] == 0) | (var_eaeb8393['fininstrmid_b'] == '')) & (var_eaeb8393['fininstrmid_b'].isnull() | (var_eaeb8393['fininstrmid'] == 0) | (var_eaeb8393['fininstrmid_b'] == ''))]
    _be39f99e = var_eaeb8393[(var_eaeb8393['src'].isnull() | (var_eaeb8393['src'] == 0) | (var_eaeb8393['src_b'] == '')) & (var_eaeb8393['src_b'].isnull() | (var_eaeb8393['src'] == 0) | (var_eaeb8393['src_b'] == ''))]
    for index, row in zx_1d1dfb4f.iterrows():
        print(f'Invalid FININSTRMID at ISIN: {row['isin']} - Both columns are empty or invalid.')
    for index, row in _be39f99e.iterrows():
        print(f'Invalid SRC at ISIN: {row['isin']} - Both columns are empty or invalid.')
    var_c164102f = pd.concat([zx_1d1dfb4f, _be39f99e])
    if not var_c164102f.empty:
        x7c4a135d = pd.to_datetime(zx_89a8394b['traddt'][0]).strftime('%d-%m-%Y')
        print(f'Invalid rows found. saving to CSV file (invalid_rows-{x7c4a135d}) and Exiting...')
        var_c164102f.to_csv(f'invalid_rows-{x7c4a135d}.csv', index=False)
        sys.exit()
    x69f08581 = xe342d3f6()
    var_f290001d = var_eaeb8393[~var_eaeb8393['isin'].isin(x69f08581)]
    if not var_f290001d.empty:
        print(f'----------Missing Data ------------')
        print(f'{var_f290001d}')
    zx_4780fc62 = {'dbname': xb4c16391, 'user': var_a0e71f8f, 'password': zx_2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{zx_4780fc62['user']}:{zx_4780fc62['password']}@{zx_4780fc62['host']}:{zx_4780fc62['port']}/{zx_4780fc62['dbname']}'
    engine = create_engine(conn_string)
    if not var_f290001d.empty:
        var_f290001d.to_sql('my_static_data', engine, if_exists='append', index=False)
        print('New static data inserted in Databse for Static Data.')
    else:
        print('No new data to insert in Databse for Static Data.')
    return var_f290001d

def sA_e8c0a0a9(zx_89a8394b):
    var_da88f560 = ['fininstrmid', 'fininstrmid_b', 'tckrsymb', 'tckrsymb_b', 'src', 'src_b', 'fininstrmnm', 'Unnamed: 0']
    sA_f33bad8d = zx_89a8394b.drop(columns=var_da88f560)
    sA_f33bad8d['traddt'] = pd.to_datetime(sA_f33bad8d['traddt'])
    xd373b5c0 = {'isin': 20, 'sctysrs': 10, 'sctysrs_b': 10, 'othr_trds': 20}
    for col, x0f82aca6 in xd373b5c0.items():
        sA_f33bad8d[col] = sA_f33bad8d[col].astype(str).str.slice(0, x0f82aca6)
    sA_68d597bb = ['ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'othr_trds_vol', 'othr_trds_txsexctd']
    for col in sA_68d597bb:
        sA_f33bad8d[col] = pd.to_numeric(sA_f33bad8d[col], errors='coerce').astype('Int64')
    sA_68d597bb = ['ttltradgvol', 'ttlnboftxsexctd', 'qty_del', 'ttltradgvol_b', 'ttlnboftxsexctd_b', 'qty_del_b', 'sum_ttltradgvol', 'sum_ttlnboftxsexctd', 'sum_del_qty', 'othr_trds_vol', 'othr_trds_txsexctd']
    for col in sA_68d597bb:
        sA_f33bad8d[col] = pd.to_numeric(sA_f33bad8d[col], errors='coerce').astype('Int64')
    x4749223f = ['opnpric', 'hghpric', 'lwpric', 'clspric', 'lastpric', 'prvsclsgpric', 'sttlmpric', 'del_per', 'avg_price', 'avg_order_price', 'close_to_avg', 'close_avg_perc']
    for col in x4749223f:
        sA_f33bad8d[col] = pd.to_numeric(sA_f33bad8d[col], errors='coerce').astype(float)
    x4749223f = ['ttltrfval', 'delvry_trnovr', 'ttltrfval_b', 'delvry_trnovr_b', 'sum_ttltrfval', 'sum_delvry_trnovr', 'del_per', 'avg_price', 'avg_qty_per_order', 'avg_order_price', 'close_to_avg', 'close_avg_perc', 'othr_trds_val']
    for col in x4749223f:
        sA_f33bad8d[col] = pd.to_numeric(sA_f33bad8d[col], errors='coerce').astype(float)
    zx_4780fc62 = {'dbname': xb4c16391, 'user': var_a0e71f8f, 'password': zx_2e48e91c, 'host': 'localhost', 'port': '5432'}
    conn_string = f'postgresql+psycopg2://{zx_4780fc62['user']}:{zx_4780fc62['password']}@{zx_4780fc62['host']}:{zx_4780fc62['port']}/{zx_4780fc62['dbname']}'
    engine = create_engine(conn_string)
    try:
        sA_f33bad8d.to_sql('my_daily_data', engine, if_exists='append', index=False)
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
    return sA_f33bad8d

def sA_7c56ebe4(zx_89a8394b):
    _f8551387(zx_89a8394b)
    sA_e8c0a0a9(zx_89a8394b)
sA_7c56ebe4(zx_89a8394b)