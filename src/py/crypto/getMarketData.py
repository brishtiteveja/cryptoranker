import requests
import pandas as pd
import os
import time
import pickle
import psutil
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import logging

from pymongo import MongoClient, UpdateOne
from datetime import datetime
import numpy as np
import logging

# get current script directory
script_dir = os.path.dirname(__file__)

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

class statsCollector:
    def __init__(self):
        self.stats = {
            'api_calls': 0,
            'api_errors': 0,
            'db_writes': 0,
            'db_errors': 0,
            'last_successful_update': None
        }

class Benchmark:
    """Context manager for timing operations"""
    def __init__(self, name, logger=None):
        self.name = name
        self.logger = logger or logging.getLogger()
        self.start = None
        self.end = None
        self.duration = 0
        self.memory_used = 0
        
    def __enter__(self):
        self.start = time.perf_counter()
        self.memory_start = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        self.logger.info(f"Starting benchmark: {self.name}")
        return self
        
    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.memory_end = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        self.duration = self.end - self.start
        self.memory_used = self.memory_end - self.memory_start
        if self.duration > 0:
            self.logger.info(f"{self.name}: {self.duration:.2f} seconds, Memory delta: {self.memory_used:.1f}MB")
        else:
            self.logger.warning(f"{self.name}: Duration is zero!")

class CryptoDataManager:
    BATCH_SIZE = 100000
    PROGRESS_INTERVAL = 10000  # Log progress every 10k documents
    
    def log_progress(self, current, total=None, operation="Processing", current_coin=None):
        """Log progress for long-running operations"""
        if total is not None and total > 0:
            percentage = (current / total) * 100
            message = f"{operation}: {current}/{total} ({percentage:.2f}%)"
        else:
            message = f"{operation}: {current} documents processed so far"
        
        if current_coin:
            message += f" - Current Coin: {current_coin}"
        
        logging.info(message)
        print(message)



    def connect_to_db(self):
        # Replace with your actual database connection logic
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017/')
        return client['crypto_db']

    # Add to CryptoDataManager:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()

    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'db') and self.db is not None:
                self.db.client.close()
        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}")
        finally:
            self.db = None

    def __del__(self):
        """Destructor to ensure cleanup"""
        self.cleanup()
    
    def __init__(self):
        self.client = MongoClient()
        self.db = self.connect_to_db()
        
        # Create indexes for better query performance
        self.db.coins.create_index("coin_id", unique=True)
        self.db.historical_data.create_index([("coin_id", 1), ("timestamp", 1)])
        self.db.categories.create_index("name", unique=True)

        self.BASE_API_URL = "https://pro-api.coingecko.com/api/v3/"
        self.COIN_LIST_API_URL = self.BASE_API_URL + "coins/list"
        self.MARKET_DATA_API_URL = self.BASE_API_URL + "coins/markets"
        self.headers={"x-cg-pro-api-key": os.getenv('COINGECKO_API_KEY')}

    def rename_field(self, collection_name="coins", old_field="category", new_field="cateogory"):
        """Rename a field in specified collection"""
        try:
            # Check document structure
            sample = self.db[collection_name].find_one()
            logging.info(f"Sample document fields: {list(sample.keys())}")

            # Perform rename with explicit query
            result = self.db[collection_name].update_many(
                {old_field: {"$exists": True}},  # Explicitly find documents with the field
                {"$rename": {old_field: new_field}},
                upsert=False
            )
            logging.info(f"Renamed {old_field} to {new_field} in {result.modified_count} documents")
            
        except Exception as e:
            logging.error(f"Error during rename: {str(e)}")
            
    def fix_missing_categories(self, coin_ids=None):
        """
        Check and fix missing categories for coins in MongoDB.
        If coin_ids is None, processes all coins in database.
        """
        try:
            # If no specific coins provided, get all coins from DB
            if coin_ids is None:
                cursor = self.db.coins.find(
                    {
                        "$or": [
                            {"category": {"$exists": False}},
                            {"category": []},
                            {"category_ranks": {"$exists": False}},
                            {"category_ranks": {}}
                        ]
                    },
                    {"coin_id": 1}
                )
                coin_ids = [doc["coin_id"] for doc in cursor]

            logging.info(f"Found {len(coin_ids)} coins with missing categories")
            
            for i, coin_id in enumerate(coin_ids):
                try:
                    # Fetch coin details from CoinGecko
                    coin_details_url = f'{self.BASE_API_URL}coins/{coin_id}'
                    logging.info(f"Fixing categories for {i}.{coin_id} with url: {coin_details_url}")
                    print(f"Fixing categories for {i}.{coin_id} with url: {coin_details_url}")
                    r = requests.get(coin_details_url, headers=self.headers)
                    
                    if r.status_code == 200:
                        coin_details = r.json()
                        coin_categories = coin_details.get('categories', [])
                        
                        # Update categories in MongoDB
                        update_doc = {
                            "$set": {
                                "category": coin_categories,
                                "updated_at": datetime.now()
                            }
                        }
                        
                        # If category_ranks is empty, initialize it
                        if not self.db.coins.find_one({"coin_id": coin_id}).get("category_ranks"):
                            category_ranks = {}
                            # Calculate rank for each category
                            for category in coin_categories:
                                # Get all coins in this category
                                category_coins = self.db.coins.find(
                                    {"category": category},
                                    {"coin_id": 1}
                                ).sort("market_cap", -1)
                                
                                # Find rank of current coin
                                for rank, cat_coin in enumerate(category_coins, 1):
                                    if cat_coin["coin_id"] == coin_id:
                                        category_ranks[category] = rank
                                        break
                            
                            update_doc["$set"]["category_ranks"] = category_ranks
                        
                        self.db.coins.update_one(
                            {"coin_id": coin_id},
                            update_doc
                        )
                        
                        logging.info(f"Updated categories for {coin_id}: {coin_categories}")
                        
                        # Respect API rate limits
                        #time.sleep(self.SLEEP_TIMER / 1000)  # Convert ms to seconds
                        
                    else:
                        logging.error(f"Error fetching details for {coin_id}: {r.status_code}")
                        
                except Exception as e:
                    logging.error(f"Error processing coin {coin_id}: {str(e)}")
                    continue
                    
        except Exception as e:
            logging.error(f"Error in fix_missing_categories: {str(e)}")
            raise
    
    def calculate_timeseries_metrics_benchmarked(self):
        try:
            benchmarks = {}
            total_benchmark = Benchmark("Total Processing")
            
            with total_benchmark:
                now = datetime.now()
                time_periods = {
                    '15min': timedelta(days=1),
                    '1h': timedelta(days=1),
                    '4h': timedelta(days=1),
                    '6h': timedelta(days=1),
                    '12h': timedelta(days=1),
                    '24h': timedelta(days=1),
                    '7d': timedelta(days=7),
                    'weekly': timedelta(weeks=1),
                    'monthly': timedelta(days=30),
                    'ytd': now - datetime(now.year, 1, 1),
                    'yearly': timedelta(days=365)
                }
                max_lookback = max(delta for delta in time_periods.values())
                earliest_needed = now - max_lookback

                # Fetch historical data from MongoDB
                pipeline = [
                    {'$match': {'timestamp': {'$gte': earliest_needed}}},
                    {'$project': {'_id': 1, 'coin_id': 1, 'timestamp': 1, 'stats.price': 1, 'stats.market_cap': 1}}
                ]
                data = list(self.db.historical_data.aggregate(pipeline, allowDiskUse=True))
                
                logging.info(f"Retrieved {len(data)} historical data documents.")
                print(f"Retrieved {len(data)} historical data documents.")
                if not data:
                    logging.warning("No historical data available for processing.")
                    return

                # Convert to DataFrame for processing
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['timestamp']).dt.normalize()
                dates = np.sort(df['date'].unique())
                coin_ids = df['coin_id'].unique()
                coin_to_idx = {coin: idx for idx, coin in enumerate(coin_ids)}

                # Prepare data arrays
                date_data = {}
                for date_idx, date in enumerate(dates):
                    self.log_progress(date_idx + 1, len(dates), operation="Processing Dates")
                    day_data = df[df['date'] == date]
                    n_coins = len(coin_ids)
                    prices = np.full(n_coins, np.nan)
                    mcaps = np.full(n_coins, np.nan)

                    for _, row in day_data.iterrows():
                        idx = coin_to_idx[row['coin_id']]
                        prices[idx] = row['stats']['price']
                        mcaps[idx] = row['stats']['market_cap']

                    ranks = np.full(n_coins, np.nan)
                    valid_mcaps = ~np.isnan(mcaps)
                    ranks[valid_mcaps] = (mcaps[valid_mcaps, None] < mcaps[valid_mcaps]).sum(axis=0) + 1
                    date_data[date] = {'prices': prices, 'mcaps': mcaps, 'ranks': ranks}

                # Calculate changes and prepare bulk updates for coins
                bulk_operations = []
                latest_date = max(dates)
                latest_data = date_data[latest_date]
                valid_latest = ~np.isnan(latest_data['prices'])

                logging.info("Starting calculation of time series metrics...")
                for period_idx, (period_name, time_delta) in enumerate(time_periods.items()):
                    self.log_progress(period_idx + 1, len(time_periods), operation="Calculating Changes for Periods")
                    logging.info(f"Processing period: {period_name}")
                    print(f"Processing period: {period_name}")

                    if period_name == 'ytd':
                        target_date = np.datetime64(datetime(now.year, 1, 1))
                    else:
                        target_date = latest_date - np.timedelta64(time_delta.days, 'D')

                    # Find the closest valid target date
                    target_date = min(dates, key=lambda x: abs(x - target_date))
                    target_data = date_data[target_date]
                    valid_both = valid_latest & ~np.isnan(target_data['prices'])

                    price_changes = np.full(len(coin_ids), np.nan)
                    price_changes[valid_both] = (
                        (latest_data['prices'][valid_both] - target_data['prices'][valid_both]) /
                        target_data['prices'][valid_both] * 100
                    )

                    rank_changes = np.full(len(coin_ids), np.nan)
                    valid_ranks = valid_both & ~np.isnan(target_data['ranks'])
                    rank_changes[valid_ranks] = (
                        target_data['ranks'][valid_ranks] - latest_data['ranks'][valid_ranks]
                    )

                    # Prepare updates for each coin
                    for idx in np.where(valid_both)[0]:
                        coin_id = coin_ids[idx]
                        changes = {
                            f'stats.change.performance_{period_name}': round(float(price_changes[idx]), 2),
                            f'stats.change.rank_{period_name}': int(rank_changes[idx]),
                        }
                        bulk_operations.append(
                            UpdateOne(
                                {"coin_id": coin_id},
                                {"$set": {**changes, "updated_at": now}},
                                upsert=True
                            )
                        )
                        logging.debug(f"Prepared update for coin {coin_id}: {changes}")
                        print(f"Prepared update for coin {coin_id}: {changes}")

                    logging.info(f"Completed changes for period: {period_name}")
                    print(f"Completed changes for period: {period_name}")

                # Execute bulk write to coins collection
                logging.info(f"Prepared {len(bulk_operations)} updates for the coins collection.")
                print(f"Prepared {len(bulk_operations)} updates for the coins collection.")
                if bulk_operations:
                    total_batches = (len(bulk_operations) + self.BATCH_SIZE - 1) // self.BATCH_SIZE
                    for batch_idx in range(0, len(bulk_operations), self.BATCH_SIZE):
                        current_batch = (batch_idx // self.BATCH_SIZE) + 1
                        self.log_progress(current_batch, total_batches, operation="Writing Updates to Coins Collection")
                        batch = bulk_operations[batch_idx:batch_idx + self.BATCH_SIZE]
                        result = self.db.coins.bulk_write(batch, ordered=False)
                        logging.info(f"Batch {current_batch}/{total_batches}: "
                                    f"Matched: {result.matched_count}, "
                                    f"Modified: {result.modified_count}, "
                                    f"Upserts: {result.upserted_count}")
                else:
                    logging.info("No updates to process for the coins collection.")
                    print("No updates to process for the coins collection.")

        except Exception as e:
            logging.error(f"Error calculating time series metrics: {str(e)}")
            raise


    def calculate_timeseries_metrics_benchmarked2(self):
        """
        Numpy-optimized version with detailed performance benchmarking.
        Now part of CryptoDataPipeline class.
        """
        try:
            benchmarks = {}
            total_benchmark = Benchmark("Total Processing")
            
            with total_benchmark:
                now = datetime.now()
                
                # Setup phase
                setup_benchmark = Benchmark("Setup")
                with setup_benchmark:
                    time_periods = {
                        '15min': timedelta(days=1),
                        '1h': timedelta(days=1),
                        '4h': timedelta(days=1),
                        '6h': timedelta(days=1),
                        '12h': timedelta(days=1),
                        '24h': timedelta(days=1),
                        '7d': timedelta(days=7),
                        'weekly': timedelta(weeks=1),
                        'monthly': timedelta(days=30),
                        'ytd': now - datetime(now.year, 1, 1),
                        'yearly': timedelta(days=365)
                    }
                    max_lookback = max(delta for delta in time_periods.values() 
                                    if isinstance(delta, timedelta))
                    earliest_needed = now - max_lookback
                benchmarks['setup'] = setup_benchmark.duration

                # MongoDB query phase
                query_benchmark = Benchmark("MongoDB Query")
                with query_benchmark:
                    pipeline = [
                        {
                            '$match': {
                                'timestamp': {'$gte': earliest_needed}
                            }
                        },
                        {
                            '$project': {
                                '_id': 1,
                                'coin_id': 1,
                                'timestamp': 1,
                                'stats.price': 1,
                                'stats.market_cap': 1
                            }
                        }
                    ]
                    data = []
                    cursor = self.db.historical_data.aggregate(pipeline, allowDiskUse=True)
                    batch = []
                    processed_docs = 0
                    for doc in cursor:
                        batch.append(doc)
                        processed_docs += 1
                        if processed_docs % self.PROGRESS_INTERVAL == 0:
                            self.log_progress(processed_docs, None, "Processing documents from MongoDB")
                        if len(batch) >= self.BATCH_SIZE:
                            data.extend(batch)
                            batch = []
                    if batch:
                        data.extend(batch)
                benchmarks['mongodb_query'] = query_benchmark.duration
                logging.info(f"Retrieved {len(data)} documents")
                print(f"Retrieved {len(data)} documents")

                # Data conversion phase
                conversion_benchmark = Benchmark("Data Conversion")
                with conversion_benchmark:
                    df = pd.DataFrame(data)
                    
                    # After DataFrame creation:
                    if df.empty:
                        raise ValueError("No data retrieved from MongoDB")

                    required_columns = ['timestamp', 'coin_id', 'stats']
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    if missing_columns:
                        raise ValueError(f"Missing required columns: {missing_columns}")

                    df['date'] = pd.to_datetime(df['timestamp']).dt.normalize()
                    # Fix: Convert to numpy array and sort
                    dates = np.sort(df['date'].unique())
                    coin_ids = df['coin_id'].unique()
                    coin_to_idx = {coin: idx for idx, coin in enumerate(coin_ids)}
                benchmarks['data_conversion'] = conversion_benchmark.duration
                logging.info(f"Processing {len(coin_ids)} unique coins")
                print(f"Processing {len(coin_ids)} unique coins")

                # Array creation phase
                # During the array creation phase
                array_benchmark = Benchmark("Array Creation")
                date_data = {}
                with array_benchmark:
                    total_dates = len(dates)
                    for date_idx, date in enumerate(dates):
                        self.log_progress(date_idx + 1, total_dates, "Processing dates")
                        day_data = df[df['date'] == date]
                        n_coins = len(coin_ids)
                        
                        prices = np.full(n_coins, np.nan)
                        mcaps = np.full(n_coins, np.nan)
                        doc_ids = np.full(n_coins, None, dtype=object)
                        
                        for _, row in day_data.iterrows():
                            current_coin = row['coin_id'] if 'coin_id' in row else 'unknown'
                            self.log_progress(date_idx + 1, total_dates, "Processing coin data", current_coin=current_coin)
                            
                            try:
                                idx = coin_to_idx[row['coin_id']]
                                if 'stats' not in row:
                                    logging.warning(f"Missing stats for coin {current_coin}")
                                    continue
                                
                                stats = row['stats']
                                if not isinstance(stats, dict):
                                    logging.warning(f"Invalid stats format for coin {current_coin}")
                                    continue
                                
                                price = stats.get('price')
                                market_cap = stats.get('market_cap')
                                
                                if price is not None:
                                    prices[idx] = price
                                if market_cap is not None:
                                    mcaps[idx] = market_cap
                                
                                doc_ids[idx] = row['_id']
                            except Exception as e:
                                logging.error(f"Error processing row for coin {current_coin}: {str(e)}")
                                continue

                        # Save date-level data
                        valid_mcaps = ~np.isnan(mcaps)
                        ranks = np.full(n_coins, np.nan)
                        ranks[valid_mcaps] = (mcaps[valid_mcaps, None] < mcaps[valid_mcaps]).sum(axis=0) + 1
                        
                        date_data[date] = {
                            'prices': prices,
                            'mcaps': mcaps,
                            'ranks': ranks,
                            'doc_ids': doc_ids
                        }
                benchmarks['array_creation'] = array_benchmark.duration

                # Change calculation phase
                calc_benchmark = Benchmark("Change Calculation")
                bulk_ops = []
                with calc_benchmark:
                    latest_date = max(dates)
                    latest_data = date_data[latest_date]
                    valid_latest = ~np.isnan(latest_data['prices'])

                    total_periods = len(time_periods)
                    for period_idx, (period_name, time_delta) in enumerate(time_periods.items()):
                        self.log_progress(period_idx + 1, total_periods, f"Calculating changes for periods")

                        if period_name == 'ytd':
                            # Convert numpy.datetime64 to a Python datetime object
                            latest_date_py = pd.Timestamp(latest_date).to_pydatetime()
                            target_date = np.datetime64(datetime(latest_date_py.year, 1, 1))
                        else:
                            target_date = latest_date - np.timedelta64(time_delta.days, 'D')

                        # Ensure `target_date` matches a valid date in `dates`
                        target_date = min(dates, key=lambda x: abs(x - target_date))
                        target_data = date_data[target_date]

                        valid_both = valid_latest & ~np.isnan(target_data['prices'])

                        price_changes = np.full(len(coin_ids), np.nan)
                        price_changes[valid_both] = (
                            (latest_data['prices'][valid_both] - target_data['prices'][valid_both]) /
                            target_data['prices'][valid_both] * 100
                        )

                        rank_changes = np.full(len(coin_ids), np.nan)
                        valid_ranks = valid_both & ~np.isnan(target_data['ranks'])
                        rank_changes[valid_ranks] = (
                            target_data['ranks'][valid_ranks] - latest_data['ranks'][valid_ranks]
                        )

                        for idx in np.where(valid_both)[0]:
                            if np.isnan(price_changes[idx]) or np.isnan(rank_changes[idx]):
                                continue

                            doc_id = latest_data['doc_ids'][idx]
                            if doc_id is None:
                                continue

                            changes = {
                                f'stats.change.performance_{period_name}': round(float(price_changes[idx]), 2),
                                f'stats.change.rank_{period_name}': int(rank_changes[idx]),
                                'updated_at': now
                            }

                            bulk_ops.append(UpdateOne(
                                {'_id': doc_id},
                                {'$set': changes}
                            ))
                benchmarks['change_calculation'] = calc_benchmark.duration
                logging.info(f"Generated {len(bulk_ops)} update operations")
                print(f"Generated {len(bulk_ops)} update operations")


                # Database update phase
                update_benchmark = Benchmark("Database Updates")
                with update_benchmark:
                    update_count = 0
                    total_batches = (len(bulk_ops) + self.BATCH_SIZE - 1) // self.BATCH_SIZE  # Round up division
                    for i in range(0, len(bulk_ops), self.BATCH_SIZE):
                        current_batch = (i // self.BATCH_SIZE) + 1
                        self.log_progress(current_batch, total_batches, "Processing database updates")
                        batch = bulk_ops[i:i + self.BATCH_SIZE]
                        result = self.db.coins.bulk_write(batch, ordered=False)
                        update_count += result.modified_count
                benchmarks['database_updates'] = update_benchmark.duration
                logging.info(f"Updated {update_count} documents")
                print(f"Updated {update_count} documents")

                # Log summary
                logging.info("\nPerformance Summary:")
                print("\nPerformance Summary:")
                logging.info("-" * 50)
                print("-" * 50)
                total_time = total_benchmark.duration
                if total_time > 0:
                    for operation, duration in benchmarks.items():
                        percentage = (duration / total_time) * 100
                        logging.info(f"{operation:20s}: {duration:6.2f}s ({percentage:5.1f}%)")
                        print(f"{operation:20s}: {duration:6.2f}s ({percentage:5.1f}%)")
                else:
                    logging.warning("Total processing time is zero. Investigate possible issues.")
                    print("Warning: Total processing time is zero. Investigate possible issues.")


                logging.info("-" * 50)
                print("-" * 50)
                logging.info(f"Total time: {total_time:.2f}s")
                print(f"Total time: {total_time:.2f}s")
                
                return {
                    'benchmarks': benchmarks,
                    'total_time': total_time,
                    'coins_processed': len(coin_ids),
                    'updates_generated': len(bulk_ops)
                }
                
        except Exception as e:
            logging.error(f"Error calculating time series metrics: {str(e)}")
            raise
        
    def calculate_performance_metrics(self):
        """
        Calculate performance metrics for all cryptocurrencies and update their stats:
        - 24h price change and rank change
        - 7d price change and rank change
        - Weekly price change and rank change (Monday to Monday)
        - Monthly price change and rank change
        """
        try:
            # Get current time
            now = datetime.now()
            
            # Define time periods for calculations
            periods = {
                '24h': now - timedelta(days=1),
                '7d': now - timedelta(days=7),
                'weekly': now - timedelta(weeks=1),
                'monthly': now - timedelta(days=30)
            }
            
            # Get all coins
            coins = self.db.coins.find({}, {'coin_id': 1})
            
            for id, coin in enumerate(coins):
                coin_id = coin['coin_id']
                performance_changes = {}
                rank_changes = {}
                logging.info(f"{id}. Calculating performance and rank change metrics for {coin_id}")
                print(f"{id}. Calculating performance and rank change metrics for {coin_id}")
                
                for period_name, start_time in periods.items():
                    try:
                        # Get current price and historical price
                        current_data = self.db.historical_data.find_one(
                            {
                                'coin_id': coin_id,
                                'timestamp': {'$lte': now}
                            },
                            sort=[('timestamp', -1)]
                        )
                        
                        historical_data = self.db.historical_data.find_one(
                            {
                                'coin_id': coin_id,
                                'timestamp': {'$lte': start_time}
                            },
                            sort=[('timestamp', -1)]
                        )
                        
                        if current_data and historical_data:
                            # Calculate price change
                            current_price = current_data['stats']['price']
                            historical_price = historical_data['stats']['price']
                            
                            if historical_price != 0:  # Avoid division by zero
                                price_change = ((current_price - historical_price) / historical_price) * 100
                            else:
                                price_change = 0
                                
                            performance_changes[f'performance_{period_name}'] = round(price_change, 2)
                            
                            # Calculate rank change by market cap
                            current_market_cap = current_data['stats']['market_cap']
                            historical_market_cap = historical_data['stats']['market_cap']
                            
                            # Get historical rank
                            historical_rank = self.db.historical_data.count_documents({
                                'timestamp': historical_data['timestamp'],
                                'stats.market_cap': {'$gt': historical_market_cap}
                            }) + 1
                            
                            # Get current rank
                            current_rank = self.db.historical_data.count_documents({
                                'timestamp': current_data['timestamp'],
                                'stats.market_cap': {'$gt': current_market_cap}
                            }) + 1
                            
                            rank_change = historical_rank - current_rank
                            rank_changes[f'rank_{period_name}'] = rank_change
                            
                    except Exception as e:
                        logging.error(f"Error calculating {period_name} metrics for {coin_id}: {str(e)}")
                        continue
                
                # Update coin document with new metrics
                if performance_changes or rank_changes:
                    self.db.coins.update_one(
                        {'coin_id': coin_id},
                        {
                            '$set': {
                                'stats.change': {
                                    **performance_changes,
                                    **rank_changes
                                },
                                'updated_at': now
                            }
                        }
                    )
                    
                logging.info(f"Updated performance metrics for {coin_id}")
                
        except Exception as e:
            logging.error(f"Error in calculate_performance_metrics: {str(e)}")
            raise

    def save_coin_metadata(self, coin_data, category_map):
        """Save coin metadata with categories"""
        # Get categories from category map if available
        coin_categories = []
        for category in category_map.values():
            if any(coin['name'] == coin_data['id'] for coin in category['coins']):
                coin_categories.append(category['name'])
                
        coin_doc = {
            "coin_id": coin_data["id"],
            "symbol": coin_data["symbol"],
            "name": coin_data["name"],
            "category": coin_categories,  # Use gathered categories
            "category_ranks": {},  # Will be updated with ranks
            "updated_at": datetime.now()
        }
        
        self.db.coins.update_one(
            {"coin_id": coin_doc["coin_id"]},
            {"$set": coin_doc},
            upsert=True
        )

    def fetch_category_list(self):
        """Fetch the category list from the API"""
        api_url = f"{self.BASE_API_URL}coins/categories/list"
        response = requests.get(api_url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def save_categories(self):
        """Save category information"""
        # Check the number of coins in the database
        coins_count = self.db.coins.count_documents({})
        if coins_count == 0:
            logging.warning("No coins found in the database.")
            return

        # Fetch the category list from the API
        category_list = self.fetch_category_list()
        category_map = {category['name']: category['category_id'] for category in category_list}

        # Aggregate and count the number of coins in each category, and include the list of coins
        pipeline = [
            {"$unwind": "$category"},
            {"$group": {
                "_id": "$category",
                "coins_count": {"$sum": 1},
                "coins": {"$push": "$coin_id"}
            }}
        ]
        
        try:
            categories_data = list(self.db.coins.aggregate(pipeline))
            if not categories_data:
                logging.warning("No categories data found. Please check the database and pipeline.")
        
            for category in categories_data:
                category_doc = {
                    "name": category["_id"],
                    "category_id": category_map.get(category["_id"], None),
                    "coins_count": category["coins_count"],
                    "coins": category["coins"],
                    "updated_at": datetime.now()
                }
                
                self.db.categories.update_one(
                    {"name": category_doc["name"]},
                    {"$set": category_doc},
                    upsert=True
                )
        except Exception as e:
            logging.error(f"Error while aggregating categories: {e}")

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

    def save_historical_data(self, coin_id, timestamp, stats):
        """
        Save historical price, volume, market cap data if it doesn't exist
        Returns: bool indicating whether data was saved
        """
        # Check if data already exists for this coin and timestamp
        existing_data = self.db.historical_data.find_one({
            "coin_id": coin_id,
            "timestamp": timestamp
        })
        
        if existing_data:
            logging.info(f"Historical data already exists for {coin_id} at {timestamp}")
            return False
            
        doc = {
            "coin_id": coin_id,
            "timestamp": timestamp,
            "stats": stats,
            "updated_at": datetime.now()
        }
        
        self.db.historical_data.update_one(
            {"coin_id": coin_id, "timestamp": timestamp},
            {"$set": doc},
            upsert=True
        )
        return True

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
                    "prices": {"$push": "$stats.price"},
                    "volumes": {"$push": "$stats.volume"},
                    "market_caps": {"$push": "$stats.market_cap"}
                }
            }
        ]
        
        historical_data = self.db.historical_data.aggregate(pipeline)
        
        # Get coin metadata with categories
        coin_metadata = list(self.db.coins.find({}, {
            "_id": 0,
            "coin_id": 1,
            "category": 1,
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
                    "category": metadata["category"],
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
                categories = data['category_data'][i]['category']
                ranks = data['category_data'][i]['ranks']
                group.attrs['category'] = str(categories)
                group.attrs['category_ranks'] = str(ranks)

    
    def get_historical_dataframe(self, years=1):
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
                    "market_cap": 1,
                    "coin_id": 1
                }
            }
        ]
        cursor = self.db.historical_data.aggregate(pipeline)
        df = pd.DataFrame(list(cursor))
        df = df.rename(columns={
            'timestamp': 'Date',
            'market_cap': 'MarketCap',
            'coin_id': 'Crypto'
        })
        # Filter out rows with zero market cap
        df = df[df['MarketCap'] != 0]
        return df

class CryptoDataCollector:

    def __init__(self):
        self.db_manager = CryptoDataManager()

        # Load environment variables
        load_dotenv()
        self.API_KEY = os.getenv('COINGECKO_API_KEY')
        if not self.API_KEY:
            raise ValueError("COINGECKO_API_KEY is missing. Please set it in the environment variables.")

        
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
        self.max_page = 201
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

    def save_historical_datapoints(self, coin_id, historical_data):
        """
        Process and save historical data points for a given coin
        
        Args:
            coin_id (str): The ID of the coin
            historical_data (dict): Dictionary containing prices, market_caps, and total_volumes
            
        Returns:
            tuple: (saved_count, error_count)
        """
        saved_count = 0
        error_count = 0
        
        try:
            # Validate data structure
            if not all(key in historical_data for key in ['prices', 'market_caps', 'total_volumes']):
                logging.error(f"Missing required data fields for {coin_id}")
                return saved_count, error_count
            
            for price_data, market_cap_data, volume_data in zip(
                historical_data['prices'],
                historical_data['market_caps'],
                historical_data['total_volumes']
            ):
                try:
                    # Convert timestamp from milliseconds to datetime
                    timestamp = datetime.fromtimestamp(price_data[0]/1000)
                    
                    # Prepare stats
                    stats = {
                        'price': price_data[1],
                        'market_cap': market_cap_data[1],
                        'volume': volume_data[1]
                    }
                    
                    # Save to MongoDB
                    doc = {
                        "coin_id": coin_id,
                        "timestamp": timestamp,
                        "stats": stats,
                        "updated_at": datetime.now()
                    }
                    
                    # Update with upsert
                    self.db_manager.db.historical_data.update_one(
                        {
                            "coin_id": coin_id,
                            "timestamp": timestamp
                        },
                        {"$set": doc},
                        upsert=True
                    )
                    saved_count += 1
                    
                except Exception as e:
                    logging.error(f"Error saving data point for {coin_id} at {timestamp}: {str(e)}")
                    error_count += 1
                    continue
                    
            logging.info(f"Processed {saved_count} points for {coin_id} with {error_count} errors")
            
        except Exception as e:
            logging.error(f"Error processing historical data for {coin_id}: {str(e)}")
            
        return saved_count, error_count

    def get_hist_market_data(self, coin_ids):
        """Fetch historical market data for multiple coins"""
        days = 365 * self.MAX_YEARS
        i = 0
        exception_count = 0
        
        while i < len(coin_ids):
            coin_id = coin_ids[i]
            logging.info(f"Getting historical market cap data for {i}.coin id = {coin_id}")
            print(f"Getting historical market cap data for {i}.coin id = {coin_id}")
            
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

                self.save_historical_datapoints(coin_id, res) 
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
        for coin_id in limited_coins_data:
            try:
                coin_details_url = f'{self.BASE_API_URL}coins/{coin_id}'
                r = requests.get(coin_details_url, headers=self.headers)
                
                if r.status_code == 200:
                    coin_details = r.json()
                    coin_categories = coin_details.get('category', [])
                    
                    # Update each category with this coin
                    for category in coin_categories:
                        if category not in self.category_map:
                            self.category_map[category] = {
                                'name': category,
                                'coins': []
                            }
                        self.category_map[category]['coins'].append({
                            'name': coin_id,
                            'category': coin_categories  # Store categories with the coin
                        })
                        
            except Exception as e:
                logging.error(f"Error processing coin {coin_id}: {e}")
                continue

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
        cache_file = script_dir + '/../../../data-backup/initial_data_cache.pkl'
        
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
        # Setup options
        self.load_or_fetch_initial_data = False
        self.collect_daily_hist_data = False
        self.must_Fetch=False
        self.save_categories=False
        self.save_category_ranks=False
        self.fix_categories=False
        self.start_scheduler=False
        self.will_daily_update=False

        self.collector = CryptoDataCollector()  # Your existing collector
        self.db_manager = CryptoDataManager()   # MongoDB manager
        self.scheduler = BlockingScheduler()
        self.stats = statsCollector()
        
        # Setup logging
        logging.basicConfig(
            filename='crypto_pipeline.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def initial_load(self,
                     save_categories=False,
                     save_category_ranks=False,
                     fix_categories=False,
                     must_Fetch=False
                     ):
        """Initial data load and setup"""
        try:
            logging.info("Starting initial data load")
            
            # Run collector to get initial data
        
            if self.load_or_fetch_initial_data:
                retries = 3
                while retries > 0:
                    try:
                        self.collector.load_or_fetch_initial_data(must_fetch=must_Fetch)
                        break
                    except Exception as e:
                        retries -= 1
                        logging.error(f"Retry {3-retries}/3: {str(e)}")
                        time.sleep(300)  # 5 minute wait between retries
            
            # Save categories to MongoDB
            if self.save_categories:
                self.db_manager.save_categories()
            
                # Save coin metadata and category rankings
                for coin_data in self.collector.todays_market_data:
                    self.db_manager.save_coin_metadata(coin_data, self.collector.category_map)

            if self.save_category_ranks:
                self.db_manager.save_category_rankings(self.collector.category_map)

            if self.fix_categories:
                self.db_manager.fix_missing_categories()
            
            logging.info("Initial data load completed")
            
        except Exception as e:
            logging.error(f"Error in initial load: {str(e)}")
            raise

    def update_performance_metrics(self):
        """Update performance metrics for all cryptocurrencies"""
        try:
            logging.info("Starting performance metrics update")
            
            # self.db_manager.calculate_performance_metrics()
            # logging.info("Completed performance metrics update")

            results = self.db_manager.calculate_timeseries_metrics_benchmarked()
            logging.info(f"Completed performance metrics update.")
            print(f"Completed performance metrics update.")

            # logging.info(f"Completed performance metrics update. Processed {results['coins_processed']} coins in {results['total_time']:.2f} seconds")
            
        except Exception as e:
            logging.error(f"Error updating performance metrics: {str(e)}")
            raise
        
    def daily_update(self):
        """Daily update job with per-coin backfill functionality"""
        try:
            logging.info(f"Starting daily update at {datetime.now()}")
            self.collector.get_market_data()
            
            for i, coin_data in enumerate(self.collector.todays_market_data):
                coin_id = coin_data["id"]
                
                # Find last update for this specific coin
                last_update = self.db_manager.db.historical_data.find_one(
                    {"coin_id": coin_id},
                    sort=[("timestamp", -1)]
                )
                
                last_date = last_update["timestamp"] if last_update else datetime.now() - timedelta(days=365)
                days_to_fetch = (datetime.now() - last_date).days
                
                logging.info(f"Processing {i+1}.{coin_id}: Last update {last_date}, need to fetch {days_to_fetch} days")
                print(f"Processing {i+1}.{coin_id}: Last update {last_date}, need to fetch {days_to_fetch} days")
                
                # Fetch missing historical data if needed
                if days_to_fetch > 1:
                    hist_data = self.collector.get_historical_daily_coin_data(
                        i, coin_id, days_to_fetch
                    )
                    if hist_data:
                        self.collector.save_historical_datapoints(coin_id, hist_data)
                
                # Update today's stats
                current_stats = {
                    'price': coin_data['current_price'],
                    'volume': coin_data['total_volume'],
                    'market_cap': coin_data['market_cap']
                }
                
                # Update historical_data
                today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                self.db_manager.db.historical_data.update_one(
                    {
                        "coin_id": coin_id,
                        "timestamp": today_midnight
                    },
                    {
                        "$set": {
                            "stats": current_stats,
                            "updated_at": datetime.now()
                        }
                    },
                    upsert=True
                )
                
                # Update coins collection
                self.db_manager.db.coins.update_one(
                    {"coin_id": coin_id},
                    {
                        "$set": {
                            "stats": current_stats,
                            "updated_at": datetime.now()
                        }
                    }
                )
                
                time.sleep(self.collector.SLEEP_TIMER / 1000)
                
            logging.info("Daily update completed successfully")
            print("Daily update completed successfully")
            
        except Exception as e:
            logging.error(f"Error in daily update: {str(e)}")
            raise

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
            self.initial_load(
                must_Fetch=self.must_Fetch,
                save_categories=self.save_categories,
                save_category_ranks=self.save_category_ranks,
                fix_categories=self.fix_categories
            )


            if self.collect_daily_hist_data:
                self.collector.get_hist_market_data(self.collector.coin_ids_in_rank)
                self.collector.save_market_data()

            # Start the scheduler
            if self.start_scheduler:
                logging.info("Starting scheduler...")
                # Setup scheduler
                self.setup_scheduler()

                self.daily_update()

                self.scheduler.start()            
            elif self.will_daily_update:
                self.daily_update()
            
            
        except (KeyboardInterrupt, SystemExit):
            logging.info("Shutting down scheduler...")
            self.scheduler.shutdown()
        except Exception as e:
            logging.error(f"Fatal error in main execution: {str(e)}")
            raise

    def copy_collection_to_new_db(self,
        source_uri,
        dest_uri,
        source_db,
        dest_db,
        source_collection,
        dest_collection
    ):
        """
        Copy a MongoDB collection from one database to another with a new name.
        
        Args:
            source_uri (str): MongoDB connection URI for source database
            dest_uri (str): MongoDB connection URI for destination database
            source_db (str): Name of the source database
            dest_db (str): Name of the destination database
            source_collection (str): Name of the source collection
            dest_collection (str): New name for the collection in destination database
        """
        try:
            # Connect to source database
            source_client = MongoClient(source_uri)
            source = source_client[source_db][source_collection]
            
            # Connect to destination database
            dest_client = MongoClient(dest_uri)
            dest = dest_client[dest_db][dest_collection]
            
            # Fetch all documents from source collection
            documents = source.find()
            
            # If documents exist, insert them into destination collection
            doc_list = list(documents)
            if doc_list:
                dest.insert_many(doc_list)
                print(f"Successfully copied {len(doc_list)} documents from {source_db}.{source_collection} to {dest_db}.{dest_collection}")
            else:
                print("No documents found in source collection")
                
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            
        finally:
            # Close connections
            source_client.close()
            dest_client.close()
def main():
    # Install APScheduler if not already installed
    # pip install apscheduler
    
    pipeline = CryptoDataPipeline()

    # Set pipeline options
    pipeline.must_Fetch=False
    pipeline.load_or_fetch_initial_data = False
    pipeline.save_categories=False
    pipeline.save_category_ranks=False

    pipeline.fix_categories=False

    pipeline.collect_daily_hist_data=False
    pipeline.start_scheduler=False
    
    pipeline.will_daily_update=False

    pipeline.update_performance_metrics()

    #pipeline.db_manager.rename_field("coins", "current_stats", "stats")

    pipeline.run()

def test():
    dm = CryptoDataCollector()
    dm.db_manager.get_historical_dataframe()

def dbmigrate():
    # Replace these with your actual MongoDB connection URIs
    SOURCE_URI = "mongodb://localhost:27017/"
    DEST_URI = "mongodb://localhost:27017/"
    
    # Your database and collection names
    SOURCE_DB = "crypto_db"
    DEST_DB = "AIAggregator"
    SOURCE_COLLECTION = "coins"
    DEST_COLLECTION = "Crypto"
    
    pipeline = CryptoDataPipeline()
    pipeline.copy_collection_to_new_db(
        SOURCE_URI,
        DEST_URI,
        SOURCE_DB,
        DEST_DB,
        SOURCE_COLLECTION,
        DEST_COLLECTION
    )

if __name__ == "__main__":
    main()
    # test()
    #dbmigrate()