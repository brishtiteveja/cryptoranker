import requests
import pprint as pp
from datetime import date, timedelta
import pandas as pd
import os
import time
import pickle
from datetime import datetime

BASE_API_URL = "https://api.coingecko.com/api/v3/"
COIN_LIST_API_URL = BASE_API_URL + "coins/list"
MARKET_DATA_API_URL = BASE_API_URL + "coins/markets"
coin_ids = []
coin_ids_in_rank = []

global_query_count = 0
hist_market_data = {}

todays_market_data = []
max_page = 10

SLEEP_TIMER=100
MIN_COUNT_TO_SLEEP=4

def get_api_response(url):
    global global_query_count

    r = requests.get(url, auth=('user', 'pass'))
    global_query_count += 1
    if r.status_code == 200:
        res = r.json()
        #pp.pprint(r.json())
    else:
        print("Error: " + str(r.status_code) + " " + r.text)
        return None

    return res

def get_all_coin_ids():
    url = COIN_LIST_API_URL
    resp = get_api_response(url)

    if resp is None:
        return None
    else:
        print(type(resp))
        for c in resp:
            coin_ids.append(c['id'])

        print(len(coin_ids))

def get_market_data():
    global global_query_count
    for page_id in range(1, max_page):
        #add = "?vs_currency=usd&order=market_cap_desc&per_page=100&page=" + str(page_id) + "&sparkline=false"
        add = "?vs_currency=usd&order=market_cap_desc&page=" + str(page_id)

        url = MARKET_DATA_API_URL + add

        if global_query_count >= MIN_COUNT_TO_SLEEP:
            print("sleeping for a minute before next data fetch...")
            global_query_count = 0
            time.sleep(SLEEP_TIMER)

        print("Getting market data for page: " + str(page_id) + " \n Request url : " + url)

        resp = get_api_response(url)

        if resp is None:
            return None
        else:
            todays_market_data.extend(resp)

        

def get_coin_ids_in_rank():
    global coin_ids_in_rank
    coin_ids_in_rank = [todays_market_data[i]["id"] for i in range(0, len(todays_market_data))]
    print("")

def get_historical_daily_coin_data(coin_id, days, interval="daily"):
    global global_query_count
    url = BASE_API_URL + "/coins/" + str(coin_id) + "/market_chart"
    currency = "usd"
    add_queries = "?vs_currency=" + currency + "&days=" + str(days) + "&interval=" + str(interval) 
    url += add_queries

    r = requests.get(url, auth=('user', 'pass'))
    global_query_count += 1
    if r.status_code == 200:
        res = r.json()
        return res
        #pp.pprint(r.json())
    else:
        print("Error: " + str(r.status_code) + " " + r.text)
        return None

def get_hist_market_data(coin_ids):
    global global_query_count
    global hist_market_data
    # 15 years
    days = 365 * 15

    n = len(coin_ids)
    i = 0

    while True:
        coin_id = coin_ids[i]
        print("Getting historical market cap data for coin id = " + coin_id)
        if coin_id not in hist_market_data:
            res = get_historical_daily_coin_data(coin_id, days)


        if global_query_count >= MIN_COUNT_TO_SLEEP:
            print("sleeping for a minute before next data fetch...")
            global_query_count = 0
            time.sleep(SLEEP_TIMER)

        # res = None meant data fetching error
        if res != None:
            hist_market_data[coin_id]  = res
            i += 1

            # now dump the current state of the data
            if not os.path.isdir('./data'):
                os.mkdir('./data')
            fname = './data/crypto_market_data_' +  'for_' + str(i) + '_coins_' + str(datetime.now().strftime("%Y%m%d_%H%M%S")) + '.pkl'
            with open(fname, 'wb') as f:
                pickle.dump(hist_market_data, f)

    print("")

# weekly date manipulation
def allsundays(year):
   d = date(year, 1, 1)                    # January 1st
   d += timedelta(days = 6 - d.weekday())  # First Sunday
   while d.year == year:
      yield d
      d += timedelta(days = 7)

def get_allsundays():
    for d in allsundays(2017):
        print(d)

def all_days(year):
    return pd.date_range(start=str(year), end=str(year+1), freq='W-MON').strftime('%m/%d/%Y').tolist()


def main():
    global hist_market_data
    get_market_data()

    # # get coin ids in rank
    get_coin_ids_in_rank()

    # # now get historical market_cap for all ranked coins for several years
    sel_coin = coin_ids_in_rank
    get_hist_market_data(sel_coin)

    # print("hello")

    fname = 'crypto_market_data_' +  str(datetime.now().strftime("%Y%m%d_%H%M%S")) + '.pkl'
    with open(fname, 'wb') as f:
        pickle.dump(hist_market_data, f)

    # now produce a csv file with all the data

    # generate the chart

if __name__ == "__main__":
    # coin_ids = ["tezos", "bitcoin", "ethereum"]
    # get_hist_market_data()

    main()

    
