import requests
import pprint as pp
from datetime import date, timedelta
import pandas as pd
import os
import time
import pickle
from datetime import datetime

from dotenv import load_dotenv

# Load the .env file into environment variables
load_dotenv()
API_KEY = os.getenv('COINGECKO_API_KEY')

# Update request headers
headers = {
    'accept': 'application/json',
    'x-cg-api-key': API_KEY
}

headers = {
    "accept": "application/json",
    "x-cg-pro-api-key": API_KEY
}

BASE_API_URL = "https://pro-api.coingecko.com/api/v3/"
COIN_LIST_API_URL = BASE_API_URL + "coins/list"
MARKET_DATA_API_URL = BASE_API_URL + "coins/markets"
coin_ids = []
coin_ids_in_rank = []

global_query_count = 0
hist_market_data = {}

categories_data =[]
category_map = {}

todays_market_data = []
max_page = 101

SLEEP_TIMER=100
MAX_YEARS=10
MIN_COUNT_TO_SLEEP=4

def get_api_response(url):
    global global_query_count

    try:
        r = requests.get(url, headers="headers", auth=('user', 'pass'))
        global_query_count += 1
        if r.status_code == 200:
            res = r.json()
            #pp.pprint(r.json())
        else:
            print("Error: " + str(r.status_code) + " " + r.text)

        return res
    except Exception as e:
        print("Exception occurred in api response: ", str(e)) 
        return e

def get_all_coin_ids():
    url = COIN_LIST_API_URL
    try:
        resp = get_api_response(url)
    except:
        pass

    if resp is None:
        return None
    else:
        print(type(resp))
        for c in resp:
            coin_ids.append(c['id'])

        print(len(coin_ids))

def get_market_data():
    global global_query_count

    exception_count = 0
    for page_id in range(1, max_page):
        #add = "?vs_currency=usd&order=market_cap_desc&per_page=100&page=" + str(page_id) + "&sparkline=false"
        add = "?vs_currency=usd&order=market_cap_desc&page=" + str(page_id)

        url = MARKET_DATA_API_URL + add

        if global_query_count >= MIN_COUNT_TO_SLEEP:
            print("sleeping for a minute before next data fetch...")
            global_query_count = 0
            time.sleep(SLEEP_TIMER)

        print("Getting market data for page: " + str(page_id) + " \n Request url : " + url)

        try:
           resp = get_api_response(url)

           if resp is None:
               pass
           else:
               todays_market_data.extend(resp)
        except Exception as e:
           print(e)
           exception_count += 1
           if exception_count < 20:
               # try same page
               page_id = page_id - 1
           else:
               exception_count = 0 
           pass

def get_coin_ids_in_rank():
    global coin_ids_in_rank
    coin_ids_in_rank = [todays_market_data[i]["id"] for i in range(0, len(todays_market_data))]
    print("")

def get_historical_daily_coin_data(i, coin_id, days, interval="daily"):
    global global_query_count
    url = BASE_API_URL + "/coins/" + str(coin_id) + "/market_chart"
    currency = "usd"
    add_queries = "?vs_currency=" + currency + "&days=" + str(days) + "&interval=" + str(interval) 
    url += add_queries

    print("Getting coin #" + str(i))

    try:
        r = requests.get(url, headers=headers, auth=('user', 'pass'))
        global_query_count += 1
        if r.status_code == 200:
            res = r.json()
            return res
            #pp.pprint(r.json())
        else:
            print("Error: " + str(r.status_code) + " " + r.text)
            return None
    except Exception as e:
        print("Exception occurred : " + str(e))
        return e
        

def get_hist_market_data(coin_ids):
    global global_query_count
    global hist_market_data
    # 15 years
    days = 365 * MAX_YEARS

    n = len(coin_ids)
    i = 0

    exception_count = 0
    while True:
        coin_id = coin_ids[i]
        print("Getting historical market cap data for coin id = " + coin_id)
        if coin_id not in hist_market_data:
            try:
                res = get_historical_daily_coin_data(i, coin_id, days)
            except Exception as e:
                exception_count += 1
                if exception_count < 20:
                    continue
                else:
                    i += 1    
                    continue

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
            
            if i % 100 == 0:
                fname = './data/crypto_market_data_' +    'for_' + str(i) + '_coins_' + str(datetime.now().strftime("%Y%m%d_%H%M%S")) + '.pkl'
                with open(fname, 'wb') as f:
                    pickle.dump(hist_market_data, f)

    print("")

# weekly date manipulation
def allsundays(year):
   d = date(year, 1, 1)                       # January 1st
   d += timedelta(days = 6 - d.weekday())  # First Sunday
   while d.year == year:
      yield d
      d += timedelta(days = 7)

def get_allsundays():
    for d in allsundays(2017):
        print(d)

def all_days(year):
    return pd.date_range(start=str(year), end=str(year+1), freq='W-MON').strftime('%m/%d/%Y').tolist()

def get_all_categories():
    global categories_data
    # Fetch categories
    while True:
        try:
            categories_url = 'https://pro-api.coingecko.com/api/v3/coins/categories'
            r = requests.get(categories_url, headers=headers)
            if r.status_code == 200:
                categories_data = r.json()
                break
            else:
                time.sleep(5)
                print("Error fetching categories: " + str(r.status_code) + " " + r.text)
        except Exception as e:
            time.sleep(5)
            print("Error fetching categories: ", e)

def map_cateogry_to_coins(limited_coins_data):
    global category_map, categories_data
    n = len(limited_coins_data)
    i = 0
    while i < n:
        coin_id = limited_coins_data[i]
        coin_details_url = f'https://pro-api.coingecko.com/api/v3/coins/{coin_id}'
        try:
            r = requests.get(coin_details_url, headers=headers)
            coin_details_data = r.json()
            
            if r.status_code != 200:
                print(f"Error fetching details for coin {coin_id}: {r.status_code} {r.text}. Sleeping...")
                time.sleep(SLEEP_TIMER)
                continue
            
            # Some coins might not have the 'categories' field
            coin_categories = coin_details_data.get('categories', [])

            for i, cat in enumerate(categories_data):
                c = categories_data[i].get('name', [])
                category_map[c] = cat
                category_map[c]['coins'] = []
            
            for category_id in category_map.keys():
                if category_id in coin_categories:
                    category_map[category_id]['coins'].append({
                        # 'id': coin_id['id'],
                        # 'symbol': coin['symbol'],
                        'name': coin_id
                    })
                    
            # Be respectful to the API's rate limit
            time.sleep(5)
            i += 1
        except Exception as e:
            print(f"Error fetching details for coin {coin_id}: {e}")
            time.sleep(5)
            print(f"Sleep and Try again..")

    pass

def main():
    global hist_market_data
    get_market_data()

    # # get coin ids in rank
    get_coin_ids_in_rank()
    
    # # now get historical market_cap for all ranked coins for several years
    sel_coin = coin_ids_in_rank

    # get coin categories
    global categories_data, category_map
    get_all_categories()
    map_cateogry_to_coins(sel_coin)
    
    print(category_map)

    fname = 'crypto_category_data_' +  str(datetime.now().strftime("%Y%m%d_%H%M%S")) + '.pkl'
    with open(fname, 'wb') as f:
        pickle.dump(category_map, f)

    get_hist_market_data(sel_coin)

    # print("hello")

    fname = 'crypto_market_data_' +  str(datetime.now().strftime("%Y%m%d_%H%M%S")) + '.pkl'
    with open(fname, 'wb') as f:
        pickle.dump(hist_market_data, f)

    # now produce a csv file with all the data

    # generate the chart

if __name__ == "__main__":
    #coin_ids = ["bitcoin", "ethereum"]
    #get_hist_market_data(coin_ids)

    main()

    
