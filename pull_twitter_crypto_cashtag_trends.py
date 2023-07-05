# -*- coding: utf-8 -*-
"""
Idea: 

0. Make twitter api work first of all...
1. generate KOL list: check all accounts I followed, get their followers, (and get their followers?) to list
2. daily count of tweets per ticker?
3. Count of how many times each account enters earliest 10% of accounts that mentioned a new token, if that token is a big trend?
4. New followers - ignore, API has low allowance for this
5. Set date range
"""

#%%
# https://developer.twitter.com/en/portal/projects-and-apps
# https://developer.twitter.com/en/docs/twitter-api/rate-limits

#%%
import tweepy
import pandas as pd
import datetime
import os
import string
import numpy as np
import openpyxl
import time
from pycoingecko import CoinGeckoAPI
import requests

date_today = str(datetime.date.today())

#%% read account and password info
with open('twitter_dev_account_info.txt', 'r') as file:
    account = file.read().split('\n')

consumer_key = account[0]
consumer_secret = account[1]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.get_authorization_url()
api = tweepy.API(auth)

#%% read KOL list 
kol_list = pd.read_excel(r'selected_kols.xlsx', sheet_name = 'selected_kols')
kol_list = kol_list.drop_duplicates()
kol_list['screen_name'] = kol_list.url.str.split(r'https://twitter.com/').str[1]
# kol_list.to_excel(r'selected_kols.xlsx', index = False, sheet_name = 'selected_kols')

#%% tried scraping followings of followings... requests too limited
'''
df = pd.DataFrame()
for followed_by_screen_name in kol_list.followed_by_screen_name.tolist():
    friend_list = []
    # followed_by_screen_name = 'WhalePumpReborn'
    for user in tweepy.Cursor(api.get_friends, screen_name=followed_by_screen_name).items(10):
        print(user.screen_name)
        friend_list.append(user)
    df_friend_list = pd.DataFrame(friend_list, columns = ['screen_name'])
    df_friend_list['followed_by_screen_name'] = followed_by_screen_name
    df = pd.concat([df, df_friend_list])
'''

#%% raw data
last_updated_raw_data = pd.read_csv(r'raw_data_twitter_ticker_trend.csv')

last_updated_date = last_updated_raw_data.date.max()
# last_updated_date = str(pd.to_datetime(last_updated_date) - datetime.timedelta(days = 1))[:10]
if pd.to_datetime(date_today) - pd.to_datetime(last_updated_date) >=datetime.timedelta(days=7):
    last_updated_date = str(datetime.date.today() + datetime.timedelta(days=-7))

#%% scrape starting from the last scrape date
scrape_result = pd.DataFrame()

for kol in kol_list.screen_name.tolist():
    print('KOL: '+kol)
    tweets = tweepy.Cursor(api.search_tweets, q='(from:'+kol+')'+' until:'+date_today+' since:'+last_updated_date+' -filter:retweets -filter:replies').items(100)
    # , include_rts=False, 
    url = []
    text = []
    user = []
    date = []
    
    for i in tweets:
        print(i)
        url.append('https://twitter.com/'+i.user.screen_name+'/status/'+str(i.id))
        text.append(i.text)
        user.append(i.user.screen_name)
        date.append(str(pd.to_datetime(i.created_at))[:10])
    
    df = pd.DataFrame({'url':url, 'text':text,'user':user,'date':date})
    scrape_result = pd.concat([scrape_result,df])
    
#%% parse ticker. 
# This can be improved with a manually maintained list, as some tweets don't use token tags
# or use emojis to indicate tokens...ok

def split_ticker_by_two_signs(string, first_sign, second_sign):
    n = len(string.split(first_sign))
    if n == 1:
        return ['No Tickers Mentioned']
    else:
        l = []
        for i in range(n-1):
            ticker = '$'+((string.split(first_sign)[i+1]).split(second_sign)[0]).upper()
            l.append(ticker)
        return l

#%%
scrape_result['ticker_list'] = scrape_result['text'].apply(lambda x: x.replace('#', '$')).apply(lambda x: split_ticker_by_two_signs(x, '$',' '))

#%%
def remove_punc(s):
    for char in string.punctuation.replace('$','…')+'。.?…\n ': # remove $, add …
        s = s.replace(char, '')
    return s

#%% some formatting and cleaning
list_final = []

for i in scrape_result[['url','ticker_list']].values:
    list_temp = [[i[0], a] for a in i[1]]
    list_final += list_temp

list_final_df = pd.DataFrame(list_final, columns = ['url','ticker'])
list_final_df['ticker'] = list_final_df['ticker'].apply(lambda x: remove_punc(x))
list_final_df = list_final_df.drop_duplicates()

list_final_df = list_final_df[list_final_df['ticker'].str.len()<=7]
list_final_df = list_final_df[list_final_df['ticker'].str[1].apply(lambda x: str(x).isupper())|list_final_df['ticker'].str[-1].apply(lambda x: str(x).isupper())|list_final_df['ticker'].str[2].apply(lambda x: str(x).isupper())]

#%%
scrape_result_final = pd.merge(scrape_result, list_final_df, how = 'left', on = 'url')
scrape_result_final['ticker'] = scrape_result_final['ticker'].fillna('No Tickers Mentioned')

scrape_result_final['ticker'] = scrape_result_final['ticker'].apply(lambda x: remove_punc(x))

#%%
scrape_result_final_with_ticker = scrape_result_final.loc[scrape_result_final['ticker']!='No Tickers Mentioned']

#%% save result
scrape_result_final = pd.concat([last_updated_raw_data, scrape_result_final_with_ticker]).sort_values(by = ['date','user'])

scrape_result_final.to_csv(r'raw_data_twitter_ticker_trend.csv',encoding='utf_8_sig', index = False)

#%%
# next step: per ticker count? top-ranked are hot topics? count of hot topics per kol?

#%% top-ranked tokens - get 3-day and 7-day top mentions 
# scrape_result_final = pd.read_csv(r'raw_data_twitter_ticker_trend.csv')
final_7_days = scrape_result_final[pd.to_datetime(scrape_result_final['date'])>=(datetime.datetime.today() - datetime.timedelta(days = 7))]
['NoTickersMentioned','GM','GN','NFT','WEB3','NATO']
#%%
final_7_days_pvt = final_7_days.groupby(['ticker'])['user'].count().reset_index(name = 'count_mentions_per_ticker')
final_7_days_pvt = final_7_days_pvt.sort_values(by = 'count_mentions_per_ticker', ascending = False)

#%%
final_3_days = scrape_result_final[pd.to_datetime(scrape_result_final['date'])>=(datetime.datetime.today() - datetime.timedelta(days = 3))]

#%%
final_3_days_pvt = final_7_days.groupby(['ticker'])['user'].count().reset_index(name = 'count_mentions_per_ticker')
final_3_days_pvt = final_3_days_pvt.sort_values(by = 'count_mentions_per_ticker', ascending = False)

#%% count mentions
#%% hot-topic mention count per kol
hot_topic_7_days_list = final_7_days_pvt[final_7_days_pvt['count_mentions_per_ticker']>np.percentile(final_7_days_pvt.count_mentions_per_ticker.tolist(),0.5)]['ticker'].tolist()

#%% 
# or do a hot UNIQUE topic?
final_7_days_unique_mentions = final_7_days[['user','ticker','date']].drop_duplicates()

mention_count_per_kol = final_7_days_unique_mentions.copy()
mention_count_per_kol['is_hot_topic'] = 0
mention_count_per_kol.loc[mention_count_per_kol['ticker'].isin(hot_topic_7_days_list),'is_hot_topic'] = 1

mention_count_per_kol = mention_count_per_kol.groupby(['user']).agg({'is_hot_topic':'sum','ticker':'count'}).reset_index()
mention_count_per_kol.columns = ['user','count_hot_topic','total_count']
mention_count_per_kol['accuracy'] = mention_count_per_kol['count_hot_topic'] / mention_count_per_kol['total_count']

mention_count_per_kol = mention_count_per_kol.sort_values(by = 'accuracy', ascending = False)

#%% who is leading in the total count of hot topic mentions?
kol_winner = mention_count_per_kol.loc[(mention_count_per_kol['accuracy'] == mention_count_per_kol['accuracy'].max())|(mention_count_per_kol['count_hot_topic'] == mention_count_per_kol['count_hot_topic'].max())]
kol_winner.loc[:, 'date'] = date_today

kol_winner_history = pd.read_excel(r'PIVOT_hot_topic_rank_per_ticker_user.xlsx', sheet_name = 'kol_winner_history')

#%%
kol_winner_final = pd.concat([kol_winner,kol_winner_history]).drop_duplicates().sort_values(by = 'date', ascending = False)

#%% get price from coingecko API
def get_cg_price_usd(ID):
    try:
        return cg.get_price(ID,'USD')[ID]['usd']
    except:
        return
    return

#%% all market data 
df_alldata = pd.DataFrame()

for i in range(10):
    all_data = requests.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page='+str(i)).json()
    df_temp = pd.DataFrame.from_dict(all_data)
    df_alldata = pd.concat([df_alldata, df_temp])
    time.sleep(3)

df_alldata = df_alldata[['id','symbol','name','current_price','market_cap','total_volume','price_change_percentage_24h']].drop_duplicates()

#%% get categories with highest total market cap changes 
cg = CoinGeckoAPI()
top_categories = cg.get_coins_categories()
top_categories = pd.DataFrame(top_categories).sort_values(by = 'market_cap_change_24h', ascending = False).head(5)
top_categories['updated_at'] = date_today
top_categories['market_cap_change_24h'] = top_categories['market_cap_change_24h']/100

all_top_categories = pd.read_excel(r'PIVOT_hot_topic_rank_per_ticker_user.xlsx', sheet_name = 'top_mkt_cap_change_cats')
all_top_categories = pd.concat([all_top_categories, top_categories])
all_top_categories = all_top_categories.sort_values(by = 'updated_at', ascending = False)

#%% get 7 top search trends on coingecko
trend = cg.get_search_trending()
trend = trend['coins']

trend_final = [[trend[x]['item']['id'], trend[x]['item']['symbol']] for x in range(7)]
trend_final = pd.DataFrame(trend_final)
trend_final.columns = ['id', 'symbol']
trend_final['price_usd'] = trend_final['id'].apply(lambda x: get_cg_price_usd(x)).round(2)
trend_final = pd.merge(trend_final, df_alldata[['id','price_change_percentage_24h']], how = 'left', on = 'id')
trend_final['updated_at'] = date_today

all_top_trends = pd.read_excel(r'PIVOT_hot_topic_rank_per_ticker_user.xlsx', sheet_name = 'top_search_trends')
all_top_trends = pd.concat([all_top_trends, trend_final])
all_top_trends = all_top_trends.sort_values(by = 'updated_at', ascending = False)

#%% save everything
workbook = openpyxl.load_workbook("PIVOT_hot_topic_rank_per_ticker_user.xlsx")
writer = pd.ExcelWriter('PIVOT_hot_topic_rank_per_ticker_user.xlsx', engine='openpyxl')
writer.book = workbook

workbook.remove(workbook['hot_topic_7_days'])
workbook.remove(workbook['hot_topic_3_days'])
workbook.remove(workbook['kol_rank'])
workbook.remove(workbook['kol_winner_history'])
workbook.remove(workbook['top_mkt_cap_change_cats']) 
workbook.remove(workbook['top_search_trends'])


final_7_days_pvt.loc[final_7_days_pvt['count_mentions_per_ticker']!=1].to_excel(writer, sheet_name = 'hot_topic_7_days', index = False)
final_3_days_pvt.loc[final_3_days_pvt['count_mentions_per_ticker']!=1].to_excel(writer, sheet_name = 'hot_topic_3_days', index = False)
mention_count_per_kol.to_excel(writer, sheet_name = 'kol_rank', index = False)
kol_winner_final.to_excel(writer, sheet_name = 'kol_winner_history', index = False)
all_top_categories.to_excel(writer, sheet_name = 'top_mkt_cap_change_cats', index = False)
all_top_trends.to_excel(writer, sheet_name = 'top_search_trends', index = False)


writer.save()
writer.close()

#%%
time.sleep(5)

#%%



















