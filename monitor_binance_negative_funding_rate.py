# -*- coding: utf-8 -*-

import requests
import pandas as pd
import datetime
import os
import hmac
import base64
import math 
import sqlite3
import numpy as np
import time

'''
idea: 
pull pairs whose funding rate is currently negative, or has been negative for the past 8/16/24 hours
(funding rate updates every 8h)
'''

#%% read api_key and password
with open(r'binance_api.txt','r') as file:
    total = file.read()
    api_key = total.split('\n')[0].split("'")[1]
    api_secret = total.split('\n')[1].split("'")[1]

#%% 
from binance import Client
client = Client(api_key, api_secret)

#%% 
# convert datetime of now minus x hours to epoch time in ms
def get_epoch_time_minus_n_hour_from_now(hours=0):
    return int((pd.to_datetime(datetime.datetime.now()) - datetime.timedelta(hours = hours)).timestamp()*1000)    

# function to get funding rate
def pull_funding_rate_mult_hours(hours = [0,8,16]):
    result = pd.DataFrame()
    for i in hours:
        fr = client.futures_funding_rate(startTime=get_epoch_time_minus_n_hour_from_now(i+8),endTime=get_epoch_time_minus_n_hour_from_now(i),limit=1000)
        fr = pd.DataFrame.from_records(fr)
        fr['fundingTime'] = fr['fundingTime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
        fr['fundingRate'] = fr['fundingRate'].astype(float)
        result = pd.concat([result, fr])
        result.reset_index(drop = True, inplace = True)
    return result
    
#%% pull funding rate and concat to raw file
result = pull_funding_rate_mult_hours()
result_master = pd.read_csv(r'RAW_perp_futures_funding_rates.csv')
result_master = pd.concat([result_master, result]).reset_index(drop = True)
result_master['fundingTime'] = pd.to_datetime(result_master['fundingTime'])
result_master = result_master[result_master['fundingTime']>=datetime.datetime.now()-datetime.timedelta(days = 180)]

#%%
result_master.drop_duplicates(inplace = True)
result_master.to_csv(r'RAW_perp_futures_funding_rates.csv', index = False)

#%% find funding rate if currently negative, and was negative since 8/16/24h ago
result_temp = result_master.loc[result_master['fundingTime']>=datetime.datetime.now()-datetime.timedelta(days = 1)-datetime.timedelta(hours = 8)]
negative_fr_tokens = result_temp.loc[(result_temp['fundingRate']<0)&(result_temp['fundingTime'] == result_temp['fundingTime'].max())]

result_temp = result_temp.loc[result_temp['fundingTime'] < result_temp['fundingTime'].max()]
negative_fr_tokens_8h = result_temp.loc[result_temp['fundingTime'] == result_temp['fundingTime'].max()]
negative_fr_tokens_8h.columns = ['symbol','ft_minus_8h','fr_minus_8h']
result_temp = result_temp.loc[result_temp['fundingTime'] < result_temp['fundingTime'].max()]
negative_fr_tokens_16h = result_temp.loc[result_temp['fundingTime'] == result_temp['fundingTime'].max()]
negative_fr_tokens_16h.columns = ['symbol','ft_minus_16h','fr_minus_16h']

result_temp = result_temp.loc[result_temp['fundingTime'] < result_temp['fundingTime'].max()]
negative_fr_tokens_24h = result_temp.loc[result_temp['fundingTime'] == result_temp['fundingTime'].max()]
negative_fr_tokens_24h.columns = ['symbol','ft_minus_24h','fr_minus_24h']

negative_fr_tokens = pd.merge(negative_fr_tokens,negative_fr_tokens_8h, how = 'left', on = 'symbol')
negative_fr_tokens = pd.merge(negative_fr_tokens,negative_fr_tokens_16h, how = 'left', on = 'symbol')
negative_fr_tokens = pd.merge(negative_fr_tokens,negative_fr_tokens_24h, how = 'left', on = 'symbol')

negative_fr_tokens['flag'] = 0
negative_fr_tokens.loc[(negative_fr_tokens['fr_minus_8h']<0)|(negative_fr_tokens['fr_minus_16h']<0)|(negative_fr_tokens['fr_minus_24h']<0), 'flag'] = 1

# set priority: always negative since 24h ago: 1, since 16h ago: 2, since 8h ago: 3, currently negative but was positive 8h ago: 4
negative_fr_tokens['flag_priority'] = 4
negative_fr_tokens.loc[(negative_fr_tokens['fr_minus_8h']<0), 'flag_priority'] = negative_fr_tokens['flag_priority']-1
negative_fr_tokens.loc[(negative_fr_tokens['fr_minus_8h']<0)&(negative_fr_tokens['fr_minus_16h']<0), 'flag_priority'] = negative_fr_tokens['flag_priority']-1
negative_fr_tokens.loc[(negative_fr_tokens['fr_minus_8h']<0)&(negative_fr_tokens['fr_minus_16h']<0)&(negative_fr_tokens['fr_minus_24h']<0), 'flag_priority'] = negative_fr_tokens['flag_priority']-1

negative_fr_tokens['perp_type'] = negative_fr_tokens['symbol'].str[-4:]
negative_fr_tokens = negative_fr_tokens[['symbol', 'perp_type', 'flag', 'flag_priority', 'fundingTime', 'fundingRate', 'ft_minus_8h', 'fr_minus_8h','ft_minus_16h', 'fr_minus_16h', 'ft_minus_24h', 'fr_minus_24h']]
negative_fr_tokens.sort_values(by = ['flag','flag_priority','fundingRate'], inplace = True)
negative_fr_tokens.reset_index(drop = True, inplace = True)

for i in ['fundingTime','ft_minus_8h','ft_minus_16h','ft_minus_24h']:
    negative_fr_tokens[i] = negative_fr_tokens[i].dt.strftime("%Y-%m-%d %H:%M:%S")

#%% why not get binance wallet data and pull info
balance = client.get_account()['balances']
balance = pd.DataFrame.from_records(balance)
balance['free'] = balance['free'].astype(float)
balance['locked'] = balance['locked'].astype(float)

def get_price_in_usd(ticker):
    try:
        price = client.get_symbol_ticker(symbol = ticker+'BUSD')['price']
    except:
        try:
            price = client.get_symbol_ticker(symbol = ticker+'USDT')['price']
        except: 
            try: 
                to_btc_price = client.get_symbol_ticker(symbol = ticker+'BTC')['price']
                btc_price = client.get_symbol_ticker(symbol = 'BTCUSDT')['price']
                price = float(btc_price)*float(to_btc_price)
            except:
                price = 0
    return float(price)

balance['total'] = balance['free']+balance['locked']
balance = balance.sort_values(by = ['total'], ascending = False).head(30)
balance['unit_price'] = balance['asset'].apply(lambda x: get_price_in_usd(x))

balance['total_price'] = balance['unit_price']*balance['total']
balance = balance.loc[balance['total_price']>=2] # filter out those tokens with ~0.0001 in the wallet..
balance['date'] = datetime.datetime.now()
balance['is_most_recent'] = 0
balance.loc[balance['date']==balance.date.max(), 'is_most_recent'] = 1

#%%
writer = pd.ExcelWriter(r'RESULT_current_negative_funding_rate_perps.xlsx', engine='xlsxwriter')
negative_fr_tokens.to_excel(writer, sheet_name='Result', index = False)  # send df to writer
worksheet = writer.sheets['Result']  # pull worksheet object
worksheet.autofilter(0, 0, negative_fr_tokens.shape[0], negative_fr_tokens.shape[1]-1)
for idx, col in enumerate(negative_fr_tokens):  # loop through all columns
    series = negative_fr_tokens[col]
    max_len = max(series.astype(str).map(len).max()+1,len(str(series.name))+6) + 1 
    worksheet.set_column(idx, idx, max_len)  # set column width
writer.save()

#%%
writer = pd.ExcelWriter(r'RESULT_binance_wallet_update.xlsx', engine='xlsxwriter')
balance.to_excel(writer, sheet_name='Result', index = False)  # send df to writer
worksheet = writer.sheets['Result']  # pull worksheet object
worksheet.autofilter(0, 0, balance.shape[0], balance.shape[1]-1)
for idx, col in enumerate(balance):  # loop through all columns
    series = balance[col]
    max_len = max(series.astype(str).map(len).max()+1,len(str(series.name))+6) + 1 
    worksheet.set_column(idx, idx, max_len)  # set column width
writer.save()
