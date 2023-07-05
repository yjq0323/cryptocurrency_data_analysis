# -*- coding: utf-8 -*-
"""
this scripts pulls tokens with top 24h %price increase, and maps them to twitter KOLs that tweeted it before. 
"""
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
#https://www.okex.com/docs-v5/en/#rest-api-market-data-get-tickers

#%% load api key and password
with open('okx_trade_bot_api.txt', 'r') as file:
    total = file.read()
    okex_key = total.split('\n')[0].split('"')[1]
    okex_secret = total.split('\n')[1].split('"')[1]
    okex_pass = total.split('\n')[2].split('"')[1]

#%% ignore
def get_time():
    now = datetime.datetime.utcnow()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"

#%% convert current time + n hours to ms
def get_time_plus_n_hr(n):
    now = datetime.datetime.utcnow() + datetime.timedelta(seconds = n*3600)
    t = now.isoformat("T", "milliseconds")
    return t + "Z"

#%%
def signature(timestamp, method, request_path, body,secret_key):
    if str(body) == '{}' or str(body) == 'None':
        body = ''
    message = str(timestamp) + str.upper(method) + request_path + str(body)
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    return base64.b64encode(d)    

#%% needed to send get requests
def get_header_plus_n_hr(n):
    body= {}
    request= 'GET'
    endpoint= '/api/spot/v5/accounts'
    header = dict()
    header['CONTENT-TYPE'] = 'application/json'
    header['OK-ACCESS-KEY'] = okex_key
    header['OK-ACCESS-SIGN'] = signature(get_time_plus_n_hr(n), request, endpoint , body, okex_secret)
    header['OK-ACCESS-TIMESTAMP'] = str(get_time_plus_n_hr(n))
    header['OK-ACCESS-PASSPHRASE'] = okex_pass
    return header

#%%
def get_ticker_df_plus_n_hr(instType, hour = 0):
    url = 'https://www.okex.com/api/v5/market/tickers?instType='+instType
    response= requests.get(url, headers=get_header_plus_n_hr(hour))
    raw_data = response.json()
    raw_data = raw_data['data']
    raw_data = pd.DataFrame(raw_data)
    raw_data['ticker'] = raw_data['instId'].str.split('-').str[0]
    raw_data['ticker_vs'] = raw_data['instId'].str.split('-').str[1]
    return raw_data

#%%
if __name__=='__main__':
    spot_now = get_ticker_df_plus_n_hr('SPOT', 0)
    spot_now['change24h'] = spot_now['bidPx'].astype(float) / spot_now['open24h'].astype(float) - 1
    spot_now_usdt = spot_now[spot_now['ticker_vs'] == 'USDT'].sort_values(by = 'change24h', ascending = False)
    spot_now_usdt['timestamp'] = pd.to_datetime(spot_now_usdt['ts'],unit='ms')
    
    #%%
    top_perf = pd.read_excel(r'okex_top_performers.xlsx', sheet_name = 'top_performers')
    
    top_today = spot_now_usdt.head(10)
    top_today = top_today[top_today['change24h']>=0.1]
    top_today = top_today[['ticker','ticker_vs','change24h','timestamp']]
    
    top_perf = pd.concat([top_perf, top_today])
    
    #%%
    top_perf.sort_values(by = 'timestamp', ascending = False).to_excel(r'okex_top_performers.xlsx', sheet_name = 'top_performers', index = False)
    top_perf['date'] = pd.to_datetime(top_perf['timestamp'])
    top_perf['date'] = top_perf['date'].dt.date
    
    #%%
    tweets = pd.read_csv(r'raw_data_twitter_ticker_trend.csv')
    tweets['date'] = pd.to_datetime(tweets['date'])
    tweets['date'] = tweets['date'].dt.date
    
    #%%
    conn = sqlite3.connect(':memory:')
    top_perf.to_sql('top_perf', conn, index=False)
    tweets.to_sql('tweets', conn, index=False)
    
    qry = '''
        select tweets.url, tweets.user, tweets.date as tweet_date, tweets.ticker, top_perf.date as pump_date
        from tweets 
        left join top_perf
        on tweets.date < top_perf.date
        and tweets.ticker = top_perf.ticker
        '''
    tweets_before_price_inc = pd.read_sql_query(qry, conn)
    
    #%%
    tweets_before_price_inc = tweets_before_price_inc.loc[tweets_before_price_inc['pump_date'].isna() == False]
    tweets_before_price_inc['pump_date'] = pd.to_datetime(tweets_before_price_inc['pump_date'])
    tweets_before_price_inc['tweet_date'] = pd.to_datetime(tweets_before_price_inc['tweet_date'])
    tweets_before_price_inc['date_diff_days'] = (tweets_before_price_inc['pump_date'] - tweets_before_price_inc['tweet_date']) / np.timedelta64(1, 'D')
    tweets_before_price_inc['date_diff_days'] = tweets_before_price_inc['date_diff_days'].astype(int)
    tweets_before_price_inc.sort_values(by = ['pump_date','date_diff_days','ticker'], ascending = [False, False, True], inplace = True)
    
    #%%
    historical_tweets_bf_pump = pd.read_excel(r'tweets_before_price_inc.xlsx', sheet_name = 'tweets_before_price_inc')
    tweets_before_pump_final = pd.concat([historical_tweets_bf_pump, tweets_before_price_inc])
    
    #%%
    tweets_before_pump_final.to_excel(r'tweets_before_price_inc.xlsx', sheet_name = 'tweets_before_pump', index = False)
    









