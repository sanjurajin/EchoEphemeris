import pandas as pd
import os
from datetime import datetime, timedelta
import calendar
import json
import numpy as np
import openpyxl as xl
cwd = os.getcwd()

def _c26e9971():
    _885780aa = os.path.join('app', 'data', 'new_trade_log.xlsx')
    zx_3b45022a = xl.load_workbook(_885780aa, data_only=True)
    sA_78af7bdb = zx_3b45022a['log_trds']
    x3a6eb079 = sA_78af7bdb.values
    columns = next(x3a6eb079)
    var_32c22048 = pd.DataFrame(x3a6eb079, columns=columns)
    # print(var_32c22048)
    return var_32c22048
 
def zx_98d53de0():
    var_32c22048 = _c26e9971()
    _e560fee5 = var_32c22048[var_32c22048['trade_type'] == 'Open']
    var_32c22048 = var_32c22048[var_32c22048['trade_type'] != 'Open']
    var_32c22048['date_open'] = pd.to_datetime(var_32c22048['date_open'], format='%d-%m-%Y').dt.date
    var_32c22048['date_close'] = pd.to_datetime(var_32c22048['date_close'], format='%d-%m-%Y').dt.date
    var_32c22048['time_open'] = pd.to_datetime(var_32c22048['time_open'], format='%H:%M:%S').dt.time
    var_32c22048['time_close'] = pd.to_datetime(var_32c22048['time_close'], format='%H:%M:%S').dt.time
    var_32c22048['open_qty'] = var_32c22048['open_qty'].astype(int)
    var_32c22048['close_qty'] = var_32c22048['close_qty'].astype(int)
    var_32c22048['wk'] = var_32c22048['wk'].astype(int)
    zx_c9b755df = ['script_type', 'symbol', 'trade_type', 'script', 'side_open', 'side_close']
    for col in zx_c9b755df:
        var_32c22048[col] = var_32c22048[col].astype(str)
    var_302a6df8 = ['open_price', 'close_price', 'total_open_pr', 'total_close_pr', 'pnl']
    for col in var_302a6df8:
        var_32c22048[col] = var_32c22048[col].astype(float)
    var_32c22048['date_open'] = pd.to_datetime(var_32c22048['date_open'])
    var_32c22048.sort_index(inplace=True)
    var_32c22048 = var_32c22048[['date_open', 'symbol', 'script_type', 'script', 'trade_type', 'wk', 'open_qty', 'open_price', 'total_open_pr', 'date_close', 'close_qty', 'close_price', 'total_close_pr', 'pnl']]
    return var_32c22048

def var_783604d0(zx_071e97da, var_45af2509, var_7deeca04):
    xe21a81b4 = {'labels': zx_071e97da, 'datasets': [{'label': var_7deeca04, 'data': var_45af2509, 'borderColor': 'rgb(75, 192, 192)', 'tension': 0.1}]}
    return xe21a81b4

def x0f7092b4():
    x3a6eb079 = zx_98d53de0()
    x71a22016 = x3a6eb079['date_open'].dt.date.unique()[-4:][0]
    sA_a57e7814 = x3a6eb079[x3a6eb079['date_open'].dt.date >= x71a22016]
    sA_4d28b5a0 = sA_a57e7814['date_open'].dt.date.unique()
    sA_29acb09c = []
    for zx_0e87632c in sA_4d28b5a0:
        sA_28cebd73 = x3a6eb079[x3a6eb079['date_open'].dt.date == zx_0e87632c]
        var_1ea50a8c = sA_28cebd73['symbol'].tolist()
        var_45af2509 = sA_28cebd73['pnl'].tolist()
        sA_8063827c = sA_28cebd73['pnl'].sum().round(1)
        var_7deeca04 = sA_28cebd73['date_open'].dt.strftime('%d-%m-%Y').tolist()[0] + ' (Rs.:' + str(sA_8063827c) + ')'
        xd226b36e = var_783604d0(var_1ea50a8c, var_45af2509, var_7deeca04)
        var_24ac9261 = [0] + xd226b36e['datasets'][0]['data']
        xd226b36e['datasets'][0]['data'] = var_24ac9261
        x97207f88 = ['0'] + xd226b36e['labels']
        xd226b36e['labels'] = x97207f88
        sA_29acb09c.append(xd226b36e)
    return sA_29acb09c
def _fbx8724s ():
    wr_234jkla = os.path.join(cwd,'app', 'creden.txt')
    sa_631aada4 = open(wr_234jkla, 'r').read().split()[3]
    return float(sa_631aada4)
def zx_64439cbd():
    var_93bcfcb8 = zx_98d53de0()
    xdc383d98 = var_93bcfcb8.groupby('date_open')['pnl'].sum().reset_index()
    var_1ad6ff9a  =_fbx8724s ()
    xdc383d98['Equity'] = (var_1ad6ff9a + xdc383d98['pnl'].cumsum()).round(2)
    d_08b48570_ = xdc383d98['date_open'].dt.strftime('%Y-%m-%d').tolist()
    x30703a5c = xdc383d98['Equity'].tolist()
    return d_08b48570_, x30703a5c