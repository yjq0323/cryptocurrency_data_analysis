# -*- coding: utf-8 -*-
"""
This script returns Binance Token-USD prices ranked by %change between 2 given timestamps
"""
import pandas as pd
import datetime
from binance.client import Client
import time 
import os
import numpy as np
pd.set_option('display.max_rows', None)

#%% load api key and password
with open(r'binance_api.txt','r') as file:
    total = file.read()
    api_key = total.split('\n')[0].split("'")[1]
    api_secret = total.split('\n')[1].split("'")[1]

client = Client(api_key, api_secret)

#%% 
# convert datetime to epoch time in ms
def datetime_to_timestamp(date_time):
    if type(date_time)==str:
        timestamp = time.mktime(datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S").timetuple())*1000
    else:
        timestamp = time.mktime(date_time.timetuple())*1000        
    return timestamp

# get selected token prices at a given time
def prices_by_usd(input_timestamp):  
    if type(input_timestamp) not in [float,int]:
        timestamp = datetime_to_timestamp(input_timestamp)
    else: 
        timestamp = input_timestamp
    symbols = pd.read_excel(r'Binance Tickers.xlsx') # load selected pairs
    prices = {}
    
    for ticker in symbols['symbol'].tolist(): # pull price
        ticker_data = client.get_historical_klines(
        symbol=ticker,
        interval=Client.KLINE_INTERVAL_1MINUTE,
        start_str=str(int(timestamp)),
        end_str=str(int(timestamp+60000)),
        limit=1)# open_time,O,H,L,C,Vol,close_time,quote_asset_vol,#trades,taker_buy_base_asset_vol,taker_quote_base_asset_vol,ignore
        
        if len(ticker_data)==1:
            ticker_data = ticker_data[0] # sometimes returned data is a list within a list. In that case, take the 1st element
            
        if len(ticker_data)>=1: 
            price = float(ticker_data[4])  # 4 is closed price
            adj_symbol_name = ticker[:-4]+'USD' # all selected pairs are either BUSD or USDT ones. Therefore adjust all pair names to USD.
            prices[adj_symbol_name] = price
        else:
            prices[adj_symbol_name] = np.nan
    df = pd.DataFrame(prices.items(),columns=['pair', 'price']).drop_duplicates(subset = ['pair'], keep = 'first') # drop dup in case there are duplicates in the selected pair list.
    
    return df

def prices_ranked_by_uplift(start_timestamp, end_timestamp, limit, order): # get all prices for USD pairs at 2 given timestamps and compare
    if end_timestamp == '':
        end_timestamp = datetime.datetime.now()
        
    try: # start_timestamp allows both timestamp and # of hours, convert this to float in case the input is # of hours
        start_timestamp = float(start_timestamp)
    except:
        pass
    
    try: # limit limits output to top X rows
        limit = int(limit)
    except:
        pass
    if order=='': # determines whether to sort %change by ASC or DESC. default is DESC
        order=False

    # get timestamp and pull price data. Takes 1.5min for 200 pairs (400 GET requests). API GET limit: 2400/minute.
    if type(start_timestamp) == float: 
        start_timestamp = datetime_to_timestamp(datetime.datetime.now()-datetime.timedelta(hours = start_timestamp))        
    else: 
        start_timestamp = datetime_to_timestamp(start_timestamp)

    end_timestamp = datetime_to_timestamp(end_timestamp)
    
    df_start = prices_by_usd(start_timestamp)
    df_end = prices_by_usd(end_timestamp)

    # join both tables and get uplift
    result = pd.merge(df_start, df_end, how = 'left', on = 'pair', suffixes = ('_start','_end'))
    result['uplift'] = round(result['price_end'] / result['price_start'] -1,4)
    
    if order in ['n','N','F','f','0','No','no','False','false','x','X']:
        order = True
    else: 
        order = False
        
    result = result.sort_values(by = 'uplift', ascending = order)
    
    if type(limit) == int: 
        return result.head(limit)
    return result

#%%
if __name__=='__main__':
    while True:
        start_timestamp = input('Start time (yyyy-mm-dd hh:mm:ss), or X hours ago (int/float): ')
        end_timestamp = input('End time (yyyy-mm-dd hh:mm:ss, default: now): ')
        order = input('%change in DESC? (default: Y): ')
        limit = input('Row limit (default: N/A): ')
        a = datetime.datetime.now()
        temp = prices_ranked_by_uplift(start_timestamp, end_timestamp,limit=limit,order=order)
        b=datetime.datetime.now()

        if end_timestamp=='':
            end_timestamp=datetime.datetime.now()        
        temp['start_timestamp'] = start_timestamp
        temp['end_timestamp'] = end_timestamp
        print(temp)
        # save result to file, and add datetime in file name to avoid overwritting other files

        temp.to_excel(r'TEMP_price_change_perc_{}.xlsx'.format(str(datetime.datetime.now())[:19].replace(' ','_').replace(':','_')), index = False)
        print('Table Saved')
        