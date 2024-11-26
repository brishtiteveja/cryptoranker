"""
Improved version of the crypto analytics system with better error handling,
rate limiting, and additional features.
"""

import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import backoff
from ratelimit import limits, sleep_and_retry
from abc import ABC, abstractmethod

@dataclass
class APIConfig:
    base_url: str = "https://pro-api.coingecko.com/api/v3/"
    rate_limit_calls: int = 50
    rate_limit_period: int = 60
    max_retries: int = 3
    retry_delay: int = 5

class MetricsCollector:
    def __init__(self):
        self.metrics = {
            'api_calls': 0,
            'api_errors': 0,
            'db_writes': 0,
            'db_errors': 0,
            'last_successful_update': None,
            'processing_times': [],
            'memory_usage': [],
            'active_connections': 0
        }
    
    def log_metric(self, metric_name: str, value: any):
        if metric_name in self.metrics:
            if isinstance(self.metrics[metric_name], list):
                self.metrics[metric_name].append(value)
            else:
                self.metrics[metric_name] = value

class BaseDataSource(ABC):
    @abstractmethod
    async def fetch_data(self):
        pass
    
    @abstractmethod
    async def process_data(self):
        pass

class CoinGeckoAPI(BaseDataSource):
    def __init__(self, config: APIConfig, api_key: str):
        self.config = config
        self.session = None
        self.headers = {"x-cg-pro-api-key": api_key}
    
    @sleep_and_retry
    @limits(calls=50, period=60)
    @backoff.on_exception(backoff.expo, 
                         (aiohttp.ClientError, asyncio.TimeoutError),
                         max_tries=3)
    async def fetch_data(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        if not self.session:
            self.session = aiohttp.ClientSession(headers=self.headers)
        
        url = f"{self.config.base_url}{endpoint}"
        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()

class DataProcessor:
    def __init__(self, db_manager: 'CryptoDataManager', metrics: MetricsCollector):
        self.db_manager = db_manager
        self.metrics = metrics
        
    async def process_market_data(self, market_data: List[Dict]):
        """Process and store market data with improved error handling"""
        for data in market_data:
            try:
                # Validate data
                self._validate_market_data(data)
                
                # Calculate additional metrics
                processed_data = self._calculate_metrics(data)
                
                # Store in database
                await self.db_manager.save_historical_data(
                    coin_id=processed_data['id'],
                    timestamp=datetime.now(),
                    metrics=processed_data['metrics']
                )
                
                self.metrics.log_metric('db_writes', 1)
                
            except Exception as e:
                self.metrics.log_metric('db_errors', 1)
                logging.error(f"Error processing market data: {str(e)}")
    
    def _validate_market_data(self, data: Dict) -> bool:
        """Validate required fields in market data"""
        required_fields = ['id', 'current_price', 'total_volume', 'market_cap']
        return all(field in data for field in required_fields)
    
    def _calculate_metrics(self, data: Dict) -> Dict:
        """Calculate additional metrics from market data"""
        return {
            'id': data['id'],
            'metrics': {
                'price': data['current_price'],
                'volume': data['total_volume'],
                'market_cap': data['market_cap'],
                'price_change_24h': data.get('price_change_24h', 0),
                'volume_change_24h': data.get('volume_change_24h', 0),
                'market_cap_change_24h': data.get('market_cap_change_24h', 0),
                'volatility': self._calculate_volatility(data),
                'liquidity_score': self._calculate_liquidity_score(data)
            }
        }

class AnalyticsEngine:
    """New class for advanced analytics capabilities"""
    
    def __init__(self, db_manager: 'CryptoDataManager'):
        self.db_manager = db_manager
    
    async def calculate_market_metrics(self, timeframe: str = '24h') -> Dict:
        """Calculate overall market metrics"""
        pipeline = [
            {
                '$match': {
                    'timestamp': {
                        '$gte': self._get_timeframe_start(timeframe)
                    }
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_market_cap': {'$sum': '$metrics.market_cap'},
                    'total_volume': {'$sum': '$metrics.volume'},
                    'avg_price_change': {'$avg': '$metrics.price_change_24h'}
                }
            }
        ]
        
        return await self.db_manager.aggregate('historical_data', pipeline)
    
    async def get_top_movers(self, n: int = 10, metric: str = 'price_change_24h') -> List[Dict]:
        """Get top n coins by specified metric"""
        pipeline = [
            {
                '$sort': {f'metrics.{metric}': -1}
            },
            {
                '$limit': n
            }
        ]
        
        return await self.db_manager.aggregate('historical_data', pipeline)
    
    def _get_timeframe_start(self, timeframe: str) -> datetime:
        """Convert timeframe string to datetime"""
        units = {'h': 'hours', 'd': 'days', 'w': 'weeks'}
        value = int(timeframe[:-1])
        unit = units[timeframe[-1]]
        
        return datetime.now() - timedelta(**{unit: value})

class CryptoDataPipeline:
    def __init__(self, config: APIConfig):
        self.config = config
        self.api = CoinGeckoAPI(config, os.getenv('COINGECKO_API_KEY'))
        self.db_manager = CryptoDataManager()
        self.processor = DataProcessor(self.db_manager, MetricsCollector())
        self.analytics = AnalyticsEngine(self.db_manager)
        
    async def run_pipeline(self):
        """Main pipeline execution with improved error handling and monitoring"""
        try:
            # Fetch market data
            market_data = await self.api.fetch_data('coins/markets', {'vs_currency': 'usd'})
            
            # Process and store data
            await self.processor.process_market_data(market_data)
            
            # Calculate analytics
            market_metrics = await self.analytics.calculate_market_metrics()
            top_movers = await self.analytics.get_top_movers()
            
            # Export results if needed
            await self.db_manager.export_to_hdf5()
            
        except Exception as e:
            logging.error(f"Pipeline error: {str(e)}")
            # Implement notification system here
            raise