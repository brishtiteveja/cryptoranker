import requests
import pandas as pd
import os
import time
import pickle
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import logging

from pymongo import MongoClient
from datetime import datetime
import numpy as np
import logging

config = {
    'api': {
        'sleep_timer': 100,
        'max_retries': 20,
        'batch_size': 100
    },
    'mongodb': {
        'uri': 'mongodb://localhost:27017',
        'db_name': 'crypto_db'
    },
    'schedule': {
        'daily_update_hour': 0,
        'daily_update_minute': 5
    }
}

class MetricsCollector:
    def __init__(self):
        self.metrics = {
            'api_calls': 0,
            'api_errors': 0,
            'db_writes': 0,
            'db_errors': 0,
            'last_successful_update': None
        }

class CryptoDataManager:
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client.crypto_db
        
        # Create indexes for better query performance
        self.db.coins.create_index("coin_id", unique=True)
        self.db.historical_data.create_index([("coin_id", 1), ("timestamp", 1)])
        self.db.categories.create_index("name", unique=True)

    def save_coin_metadata(self, coin_data):
        """Save coin metadata with categories"""
        coin_doc = {
            "coin_id": coin_data["id"],
            "symbol": coin_data["symbol"],
            "name": coin_data["name"],
            "categories": coin_data.get("categories", []),
            "category_ranks": {},  # Will be updated with ranks
            "updated_at": datetime.now()
        }
        
        self.db.coins.update_one(
            {"coin_id": coin_doc["coin_id"]},
            {"$set": coin_doc},
            upsert=True
        )

    def save_categories(self, categories_data):
        """Save category information"""
        for category in categories_data:
            category_doc = {
                "name": category["name"],
                "market_cap": category.get("market_cap", 0),
                "volume_24h": category.get("volume_24h", 0),
                "coins_count": category.get("coins_count", 0),
                "updated_at": datetime.now()
            }
            
            self.db.categories.update_one(
                {"name": category_doc["name"]},
                {"$set": category_doc},
                upsert=True
            )

    def save_category_rankings(self, category_map):
        """Save coin rankings within categories"""
        for category_name, category_data in category_map.items():
            # Update category info
            self.db.categories.update_one(
                {"name": category_name},
                {
                    "$set": {
                        "market_cap": category_data.get("market_cap", 0),
                        "volume_24h": category_data.get("volume_24h", 0),
                        "updated_at": datetime.now()
                    }
                }
            )
            
            # Update coin rankings in this category
            for rank, coin in enumerate(category_data.get("coins", []), 1):
                self.db.coins.update_one(
                    {"coin_id": coin["name"]},
                    {
                        "$set": {
                            f"category_ranks.{category_name}": rank,
                            "updated_at": datetime.now()
                        }
                    }
                )

    def save_historical_data(self, coin_id, timestamp, metrics):
        """Save historical price, volume, market cap data"""
        doc = {
            "coin_id": coin_id,
            "timestamp": timestamp,
            "metrics": metrics,
            "updated_at": datetime.now()
        }
        
        self.db.historical_data.update_one(
            {"coin_id": coin_id, "timestamp": timestamp},
            {"$set": doc},
            upsert=True
        )

    def health_check(self):
        try:
            # Check MongoDB connection
            self.client.server_info()  # Fixed from self.db_manager
            
            # Check API access
            test_url = "https://pro-api.coingecko.com/api/v3/ping"  # Hardcoded URL is fine for health check
            response = requests.get(test_url, headers={"x-cg-pro-api-key": os.getenv('COINGECKO_API_KEY')})
            if response.status_code != 200:
                raise Exception("API not responding")
                
            return True
        except Exception as e:
            logging.error(f"Health check failed: {str(e)}")
            return False

    def get_ml_ready_data(self, start_date, end_date):
        """Get data in ML-ready format"""
        # Get historical data
        pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }
            },
            {
                "$sort": {"timestamp": 1}
            },
            {
                "$group": {
                    "_id": "$coin_id",
                    "prices": {"$push": "$metrics.price"},
                    "volumes": {"$push": "$metrics.volume"},
                    "market_caps": {"$push": "$metrics.market_cap"}
                }
            }
        ]
        
        historical_data = self.db.historical_data.aggregate(pipeline)
        
        # Get coin metadata with categories
        coin_metadata = list(self.db.coins.find({}, {
            "_id": 0,
            "coin_id": 1,
            "categories": 1,
            "category_ranks": 1
        }))
        
        # Prepare ML features
        features = []
        coin_ids = []
        category_data = []
        
        for hist in historical_data:
            coin_id = hist["_id"]
            metadata = next((m for m in coin_metadata if m["coin_id"] == coin_id), None)
            
            if metadata:
                coin_ids.append(coin_id)
                features.append(np.stack([
                    hist["prices"],
                    hist["volumes"],
                    hist["market_caps"]
                ], axis=-1))
                category_data.append({
                    "categories": metadata["categories"],
                    "ranks": metadata["category_ranks"]
                })
        
        return {
            "features": np.array(features),
            "coin_ids": np.array(coin_ids),
            "category_data": category_data
        }

    def export_to_hdf5(self, filename='crypto_ml_data.h5'):
        """Export data to HDF5 format for ML"""
        import h5py
        
        data = self.get_ml_ready_data(
            start_date=datetime.now() - timedelta(days=365*10),
            end_date=datetime.now()
        )
        
        with h5py.File(filename, 'w') as f:
            f.create_dataset('features', data=data['features'])
            f.create_dataset('coin_ids', data=data['coin_ids'].astype('S'))  # Store as strings
            
            # Store category information as attributes
            for i, coin_id in enumerate(data['coin_ids']):
                group = f.create_group(f'coin_{i}_categories')
                categories = data['category_data'][i]['categories']
                ranks = data['category_data'][i]['ranks']
                group.attrs['categories'] = str(categories)
                group.attrs['category_ranks'] = str(ranks)

    def get_historical_dataframe(years=1):
        start_date = datetime.now() - timedelta(days=365 * years)
        pipeline = [
            {
                "$match": {
                    "timestamp": {"$gte": start_date}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "timestamp": 1,
                    "coin_id": 1,
                    "price": "$stats.price",
                    "market_cap": "$stats.market_cap",
                    "volume": "$stats.volume"
                }
            }
        ]
        cursor = self.db.historical_data.aggregate(pipeline)
        df = pd.DataFrame(list(cursor))
        df = df.rename(columns={
            'timestamp': 'Date',
            'coin_id': 'Crypto',
            'market_cap': 'MarketCap',
            'price': 'Price',
            'volume': 'Volume'
        })
        # Filter out rows with zero market cap
        df = df[df['MarketCap'] != 0]
        return df

class CryptoDataCollector:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        self.API_KEY = os.getenv('COINGECKO_API_KEY')
        
        # API Configuration
        self.BASE_API_URL = "https://pro-api.coingecko.com/api/v3/"
        self.COIN_LIST_API_URL = self.BASE_API_URL + "coins/list"
        self.MARKET_DATA_API_URL = self.BASE_API_URL + "coins/markets"
        
        # Request headers
        self.headers = {
            "accept": "application/json",
            "x-cg-pro-api-key": self.API_KEY
        }
        
        # Data storage
        self.coin_ids = []
        self.coin_ids_in_rank = []
        self.hist_market_data = {}
        self.categories_data = []
        self.category_map = {}
        self.todays_market_data = []
        
        # Configuration
        self.max_page = 101
        self.SLEEP_TIMER = 100
        self.MAX_YEARS = 10
        self.MIN_COUNT_TO_SLEEP = 101
        self.global_query_count = 0
        
        # Setup logging
        logging.basicConfig(
            filename='crypto_collector.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Create data directory
        if not os.path.isdir('./data'):
            os.mkdir('./data')

    def get_api_response(self, url):
        """Generic API request handler"""
        try:
            r = requests.get(url, headers=self.headers)
            self.global_query_count += 1
            if r.status_code == 200:
                return r.json()
            else:
                logging.error(f"Error: {r.status_code} {r.text}")
                return None
        except Exception as e:
            logging.error(f"Exception occurred in api response: {str(e)}")
            return e

    def get_all_coin_ids(self):
        """Fetch all available coin IDs"""
        try:
            resp = self.get_api_response(self.COIN_LIST_API_URL)
            if resp:
                self.coin_ids = [c['id'] for c in resp]
                logging.info(f"Retrieved {len(self.coin_ids)} coin IDs")
            return resp
        except Exception as e:
            logging.error(f"Error getting coin IDs: {str(e)}")
            return None

    def get_market_data(self):
        """Fetch current market data for all coins"""
        exception_count = 0
        for page_id in range(1, self.max_page):
            if self.global_query_count >= self.MIN_COUNT_TO_SLEEP:
                logging.info("Sleeping before next data fetch...")
                self.global_query_count = 0
                time.sleep(self.SLEEP_TIMER)
            
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'page': page_id
            }
            url = f"{self.MARKET_DATA_API_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
            print(f"Getting market data for page: {page_id}\nRequest url: {url}")
            logging.info(f"Getting market data for page: {page_id}\nRequest url: {url}")
            
            try:
                resp = self.get_api_response(url)
                if resp:
                    self.todays_market_data.extend(resp)
            except Exception as e:
                logging.error(str(e))
                exception_count += 1
                if exception_count < 20:
                    page_id -= 1
                else:
                    exception_count = 0

    def get_coin_ids_in_rank(self):
        """Extract coin IDs from market data"""
        self.coin_ids_in_rank = [data["id"] for data in self.todays_market_data]

    def get_historical_daily_coin_data(self, i, coin_id, days, interval="daily"):
        """Fetch historical data for a specific coin"""
        url = f"{self.BASE_API_URL}/coins/{coin_id}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': days,
            'interval': interval
        }
        url += f"?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        
        logging.info(f"Getting coin #{i}: {coin_id}")
        return self.get_api_response(url)

    def get_hist_market_data(self, coin_ids):
        """Fetch historical market data for multiple coins"""
        days = 365 * self.MAX_YEARS
        i = 0
        exception_count = 0
        
        while i < len(coin_ids):
            coin_id = coin_ids[i]
            logging.info(f"Getting historical market cap data for coin id = {coin_id}")
            
            if coin_id not in self.hist_market_data:
                try:
                    res = self.get_historical_daily_coin_data(i, coin_id, days)
                except Exception as e:
                    exception_count += 1
                    if exception_count < 20:
                        continue
                    else:
                        i += 1
                        continue

            if self.global_query_count >= self.MIN_COUNT_TO_SLEEP:
                logging.info("Sleeping before next data fetch...")
                self.global_query_count = 0
                time.sleep(self.SLEEP_TIMER)

            if res is not None:
                self.hist_market_data[coin_id] = res
                i += 1

                if i % 100 == 0:
                    self._save_checkpoint(i)

    def _save_checkpoint(self, i):
        """Save current progress to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f'./data/crypto_market_data_for_{i}_coins_{timestamp}.pkl'
        with open(fname, 'wb') as f:
            pickle.dump(self.hist_market_data, f)

    @staticmethod
    def allsundays(year):
        """Generate all Sundays for a given year"""
        d = date(year, 1, 1)
        d += timedelta(days=6 - d.weekday())
        while d.year == year:
            yield d
            d += timedelta(days=7)

    def get_allsundays(self):
        """Print all Sundays for 2017"""
        for d in self.allsundays(2017):
            print(d)

    @staticmethod
    def all_days(year):
        """Generate all Mondays for a given year"""
        return pd.date_range(start=str(year), end=str(year+1), freq='W-MON').strftime('%m/%d/%Y').tolist()

    def get_all_categories(self):
        """Fetch all cryptocurrency categories"""
        while True:
            try:
                categories_url = f'{self.BASE_API_URL}coins/categories'
                r = requests.get(categories_url, headers=self.headers)
                if r.status_code == 200:
                    self.categories_data = r.json()
                    break
                else:
                    time.sleep(5)
                    logging.error(f"Error fetching categories: {r.status_code} {r.text}")
            except Exception as e:
                time.sleep(5)
                logging.error(f"Error fetching categories: {e}")

    def map_category_to_coins(self, limited_coins_data):
        """Map coins to their categories"""
        n = len(limited_coins_data)
        coin_index = 0  # renamed from i to avoid conflict
        err_cnt = 0
        
        while coin_index < n:
            coin_id = limited_coins_data[coin_index]
            coin_details_url = f'{self.BASE_API_URL}coins/{coin_id}'
            try:
                logging.info(f"{coin_index + 1}. Fetching details for coin {coin_id}")
                print(f"{coin_index + 1}. Fetching details for coin {coin_id}")
                r = requests.get(coin_details_url, headers=self.headers)
                coin_details_data = r.json()
                
                if r.status_code != 200:
                    logging.error(f"Error fetching details for coin {coin_id}: {r.status_code} {r.text}")
                    print(f"Error fetching details for coin {coin_id}: {r.status_code} {r.text}")
                    time.sleep(self.SLEEP_TIMER)
                    continue
                
                coin_categories = coin_details_data.get('categories', [])
                
                # Changed this loop to use different variable
                for cat_idx, cat in enumerate(self.categories_data):
                    c = self.categories_data[cat_idx].get('name', [])
                    self.category_map[c] = cat
                    self.category_map[c]['coins'] = []
                
                for category_id in self.category_map.keys():
                    if category_id in coin_categories:
                        self.category_map[category_id]['coins'].append({'name': coin_id})
                
                coin_index += 1  # increment the main counter
                err_cnt = 0  # reset error count on success
                
            except Exception as e:
                err_cnt += 1
                logging.error(f"Error fetching details for coin {coin_id}: {e}")
                print(f"Error fetching details for coin {coin_id}: {e}")
                if err_cnt >= 3:
                    coin_index += 1  # move to next coin after 3 errors
                    err_cnt = 0  # reset error count

    def save_category_data(self):
        """Save category mapping data"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f'crypto_category_data_{timestamp}.pkl'
        with open(fname, 'wb') as f:
            pickle.dump(self.category_map, f)

    def save_market_data(self):
        """Save final market data"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f'crypto_market_data_{timestamp}.pkl'
        with open(fname, 'wb') as f:
            pickle.dump(self.hist_market_data, f)

    def load_or_fetch_initial_data(self, must_fetch=False):
        """Load initial data from cache or fetch if not available"""
        cache_file = './data/initial_data_cache.pkl'
        
        # Try to load from cache
        if not must_fetch and os.path.exists(cache_file):
            try:
                with open(cache_file, 'rb') as f:
                    cached_data = pickle.load(f)
                    logging.info("Loaded initial data from cache")
                    
                    # Restore data from cache
                    self.todays_market_data = cached_data['market_data']
                    self.coin_ids_in_rank = cached_data['coin_ids_in_rank']
                    self.categories_data = cached_data['categories_data']
                    self.category_map = cached_data['category_map']
                    
                    return True
            except Exception as e:
                logging.error(f"Error loading cache: {e}")
                return False
        
        # If cache doesn't exist or failed to load, fetch fresh data
        try:
            logging.info("Fetching fresh initial data...")
            self.get_market_data()
            self.get_coin_ids_in_rank()
            self.get_all_categories()
            self.map_category_to_coins(self.coin_ids_in_rank)
            
            # Cache the fetched data
            cached_data = {
                'market_data': self.todays_market_data,
                'coin_ids_in_rank': self.coin_ids_in_rank,
                'categories_data': self.categories_data,
                'category_map': self.category_map,
                'fetch_timestamp': datetime.now()
            }
            
            with open(cache_file, 'wb') as f:
                pickle.dump(cached_data, f)
            logging.info("Saved initial data to cache")
            
            return True
        except Exception as e:
            logging.error(f"Error fetching initial data: {e}")
            return False

    def run(self):
        """Main execution method"""
        # Load or fetch initial data
        if not self.load_or_fetch_initial_data():
            logging.error("Failed to load or fetch initial data. Exiting.")
            return
        
        # Continue with historical data collection
        sel_coin = self.coin_ids_in_rank
        self.get_hist_market_data(sel_coin)
        self.save_market_data()

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
from datetime import datetime, timedelta

class CryptoDataPipeline:
    def __init__(self):
        self.collector = CryptoDataCollector()  # Your existing collector
        self.db_manager = CryptoDataManager()   # MongoDB manager
        self.scheduler = BlockingScheduler()
        self.metrics = MetricsCollector()
        
        # Setup logging
        logging.basicConfig(
            filename='crypto_pipeline.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def initial_load(self):
        """Initial data load and setup"""
        try:
            logging.info("Starting initial data load")
            
            # Run collector to get initial data
            retries = 3
        
            while retries > 0:
                try:
                    self.collector.load_or_fetch_initial_data()
                    break
                except Exception as e:
                    retries -= 1
                    logging.error(f"Retry {3-retries}/3: {str(e)}")
                    time.sleep(300)  # 5 minute wait between retries
            
            # Save categories to MongoDB
            self.db_manager.save_categories(self.collector.categories_data)
            
            # Save coin metadata and category rankings
            for coin_data in self.collector.todays_market_data:
                self.db_manager.save_coin_metadata(coin_data)
            self.db_manager.save_category_rankings(self.collector.category_map)
            
            # Save historical data
            for coin in self.collector.hist_market_data:
                self.db_manager.save_historical_data(
                    coin_id=coin['id'],
                    timestamp=datetime.now(),
                    metrics={
                        'price': coin['current_price'],
                        'volume': coin['total_volume'],
                        'market_cap': coin['market_cap']
                    }
                )
            
            logging.info("Initial data load completed")
            
        except Exception as e:
            logging.error(f"Error in initial load: {str(e)}")
            raise

    def daily_update(self):
        """Daily update job"""
        try:
            logging.info(f"Starting daily update at {datetime.now()}")
            
            # Get latest data for all coins
            self.collector.get_market_data()
            
            # Update categories if needed (weekly)
            if datetime.now().weekday() == 0:  # Monday
                self.collector.get_all_categories()
                self.collector.map_category_to_coins(self.collector.coin_ids_in_rank)
                self.db_manager.save_categories(self.collector.categories_data)
                self.db_manager.save_category_rankings(self.collector.category_map)
            
            # Update historical data for each coin
            for coin_data in self.collector.todays_market_data:
                self.db_manager.save_historical_data(
                    coin_id=coin_data['id'],
                    timestamp=datetime.now(),
                    metrics={
                        'price': coin_data['current_price'],
                        'volume': coin_data['total_volume'],
                        'market_cap': coin_data['market_cap']
                    }
                )
            
            # Export ML-ready data periodically (weekly)
            if datetime.now().weekday() == 0:
                self.db_manager.export_to_hdf5()
            
            logging.info("Daily update completed successfully")
            
        except Exception as e:
            logging.error(f"Error in daily update: {str(e)}")
            # Optionally, add retry logic or notifications here

    def setup_scheduler(self):
        """Setup the scheduler for daily updates"""
        # Run daily at 00:05 UTC
        self.scheduler.add_job(
            self.daily_update,
            CronTrigger(hour=0, minute=5),
            id='daily_crypto_update',
            max_instances=1,
            coalesce=True  # Prevent multiple runs if previous job is still running
        )
        
        logging.info("Scheduler setup completed")

    def run(self):
        """Main execution method"""
        try:
            # Initial setup and data load
            self.initial_load()
            
            # Setup scheduler
            self.setup_scheduler()
            
            # Start the scheduler
            logging.info("Starting scheduler...")
            self.scheduler.start()
            
        except (KeyboardInterrupt, SystemExit):
            logging.info("Shutting down scheduler...")
            self.scheduler.shutdown()
        except Exception as e:
            logging.error(f"Fatal error in main execution: {str(e)}")
            raise

def main():
    # Install APScheduler if not already installed
    # pip install apscheduler
    
    pipeline = CryptoDataPipeline()
    
    pipeline.db_manager.get_historical_dataframe(years=1)
    #pipeline.run()

if __name__ == "__main__":
    main()