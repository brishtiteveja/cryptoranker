import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
import talib
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

@dataclass
class PriceLevel:
    price: float
    strength: float  # 0-1 score based on number of touches and volume
    last_touch: datetime
    touches: int
    total_volume: float

@dataclass
class ATHBreakout:
    coin_id: str
    timestamp: datetime
    price: float
    volume_increase: float  # % increase in volume
    breakout_strength: float  # composite score
    previous_ath: float

class TechnicalAnalyzer:
    def __init__(self, db_manager: 'CryptoDataManager'):
        self.db = db_manager
        self.price_cache = {}  # Cache for frequently accessed price data
        
    async def analyze_ath_breakouts(self, 
                                  coin_ids: Optional[List[str]] = None,
                                  category: Optional[str] = None,
                                  lookback_days: int = 365) -> List[ATHBreakout]:
        """Detect ATH breakouts across multiple coins"""
        
        # Get relevant coin IDs based on category or input list
        if category:
            coin_ids = await self.db.get_coins_by_category(category)
        elif not coin_ids:
            coin_ids = await self.db.get_all_coin_ids()
        
        breakouts = []
        
        # Process coins in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for coin_id in coin_ids:
                futures.append(
                    executor.submit(
                        self._analyze_single_coin_ath,
                        coin_id,
                        lookback_days
                    )
                )
            
            for future in futures:
                result = future.result()
                if result:
                    breakouts.extend(result)
        
        return sorted(breakouts, key=lambda x: x.breakout_strength, reverse=True)
    
    def _analyze_single_coin_ath(self, 
                                coin_id: str,
                                lookback_days: int) -> Optional[ATHBreakout]:
        """Analyze single coin for ATH breakout"""
        try:
            # Get historical data
            price_data = self._get_price_data(coin_id, lookback_days)
            if price_data is None or len(price_data) < 2:
                return None
            
            # Calculate ATH
            prices = price_data['price'].values
            volumes = price_data['volume'].values
            timestamps = price_data.index
            
            # Calculate rolling ATH
            rolling_ath = pd.Series(prices).rolling(window=lookback_days, min_periods=1).max()
            
            # Detect breakout
            is_breakout = (prices[-1] > rolling_ath[-2])  # Current price > previous ATH
            
            if is_breakout:
                # Calculate volume increase
                avg_volume = np.mean(volumes[-30:-1])  # Average volume last 30 days
                current_volume = volumes[-1]
                volume_increase = ((current_volume - avg_volume) / avg_volume) * 100
                
                # Calculate breakout strength score
                strength = self._calculate_breakout_strength(
                    price=prices[-1],
                    prev_ath=rolling_ath[-2],
                    volume_increase=volume_increase,
                    price_history=prices,
                    volume_history=volumes
                )
                
                return ATHBreakout(
                    coin_id=coin_id,
                    timestamp=timestamps[-1],
                    price=prices[-1],
                    volume_increase=volume_increase,
                    breakout_strength=strength,
                    previous_ath=rolling_ath[-2]
                )
            
            return None
            
        except Exception as e:
            logging.error(f"Error analyzing ATH for {coin_id}: {str(e)}")
            return None
    
    def _calculate_breakout_strength(self,
                                   price: float,
                                   prev_ath: float,
                                   volume_increase: float,
                                   price_history: np.ndarray,
                                   volume_history: np.ndarray) -> float:
        """Calculate breakout strength score (0-1)"""
        # Percentage above previous ATH
        price_breakout_pct = ((price - prev_ath) / prev_ath) * 100
        
        # Volume strength
        volume_score = min(volume_increase / 200, 1.0)  # Normalize to 0-1
        
        # Price movement strength
        price_score = min(price_breakout_pct / 20, 1.0)  # Normalize to 0-1
        
        # Recent momentum (using RSI)
        rsi = talib.RSI(price_history)[-1]
        momentum_score = min(rsi / 100, 1.0)
        
        # Combine scores with weights
        weights = {
            'volume': 0.4,
            'price': 0.3,
            'momentum': 0.3
        }
        
        return (
            weights['volume'] * volume_score +
            weights['price'] * price_score +
            weights['momentum'] * momentum_score
        )

    async def find_support_resistance_levels(self,
                                          coin_ids: Optional[List[str]] = None,
                                          category: Optional[str] = None,
                                          lookback_days: int = 365,
                                          min_touches: int = 3) -> Dict[str, Dict[str, List[PriceLevel]]]:
        """Find support and resistance levels across multiple coins"""
        
        if category:
            coin_ids = await self.db.get_coins_by_category(category)
        elif not coin_ids:
            coin_ids = await self.db.get_all_coin_ids()
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for coin_id in coin_ids:
                futures.append(
                    executor.submit(
                        self._analyze_single_coin_levels,
                        coin_id,
                        lookback_days,
                        min_touches
                    )
                )
            
            for future, coin_id in zip(futures, coin_ids):
                result = future.result()
                if result:
                    results[coin_id] = result
        
        return results
    
    def _analyze_single_coin_levels(self,
                                  coin_id: str,
                                  lookback_days: int,
                                  min_touches: int) -> Optional[Dict[str, List[PriceLevel]]]:
        """Analyze support and resistance levels for a single coin"""
        try:
            # Get historical data
            price_data = self._get_price_data(coin_id, lookback_days)
            if price_data is None or len(price_data) < min_touches:
                return None
            
            prices = price_data['price'].values
            volumes = price_data['volume'].values
            timestamps = price_data.index
            
            # Find potential levels using peak detection
            support_levels = self._find_price_levels(
                prices, volumes, timestamps, 
                level_type='support',
                min_touches=min_touches
            )
            
            resistance_levels = self._find_price_levels(
                prices, volumes, timestamps,
                level_type='resistance',
                min_touches=min_touches
            )
            
            return {
                'support': support_levels,
                'resistance': resistance_levels
            }
            
        except Exception as e:
            logging.error(f"Error analyzing levels for {coin_id}: {str(e)}")
            return None
    
    def _find_price_levels(self,
                          prices: np.ndarray,
                          volumes: np.ndarray,
                          timestamps: np.ndarray,
                          level_type: str,
                          min_touches: int) -> List[PriceLevel]:
        """Find price levels using peak detection and clustering"""
        
        # Use different techniques to find potential levels
        levels = []
        
        # 1. Local minima/maxima
        if level_type == 'support':
            peaks = self._find_local_minima(prices)
        else:
            peaks = self._find_local_maxima(prices)
            
        # 2. Volume Profile
        volume_nodes = self._find_volume_nodes(prices, volumes)
        
        # 3. Historical pivots
        pivots = self._find_pivot_points(prices)
        
        # Combine all potential levels
        all_levels = np.concatenate([
            prices[peaks],
            volume_nodes,
            pivots
        ])
        
        # Cluster nearby levels
        clustered_levels = self._cluster_price_levels(all_levels)
        
        # Validate each level
        for level_price in clustered_levels:
            touches = self._count_level_touches(prices, volumes, level_price)
            if touches >= min_touches:
                # Calculate level strength
                strength = self._calculate_level_strength(
                    level_price, prices, volumes, touches
                )
                
                # Find last touch
                last_touch = timestamps[self._find_last_touch(prices, level_price)]
                
                # Calculate total volume at touches
                total_volume = self._calculate_level_volume(
                    prices, volumes, level_price
                )
                
                levels.append(PriceLevel(
                    price=level_price,
                    strength=strength,
                    last_touch=last_touch,
                    touches=touches,
                    total_volume=total_volume
                ))
        
        return sorted(levels, key=lambda x: x.strength, reverse=True)
    
    @staticmethod
    def _find_local_minima(prices: np.ndarray) -> np.ndarray:
        """Find local minima in price data"""
        return np.where((prices[1:-1] <= prices[:-2]) & 
                       (prices[1:-1] <= prices[2:]))[0] + 1
    
    @staticmethod
    def _find_local_maxima(prices: np.ndarray) -> np.ndarray:
        """Find local maxima in price data"""
        return np.where((prices[1:-1] >= prices[:-2]) & 
                       (prices[1:-1] >= prices[2:]))[0] + 1
    
    def _find_volume_nodes(self,
                          prices: np.ndarray,
                          volumes: np.ndarray,
                          num_bins: int = 100) -> np.ndarray:
        """Find price levels with high volume concentration"""
        hist, bins = np.histogram(prices, bins=num_bins, weights=volumes)
        significant_nodes = bins[:-1][hist > np.mean(hist) + np.std(hist)]
        return significant_nodes
    
    @staticmethod
    def _find_pivot_points(prices: np.ndarray) -> np.ndarray:
        """Calculate pivot points"""
        highs = pd.Series(prices).rolling(window=20).max()
        lows = pd.Series(prices).rolling(window=20).min()
        pivot = (highs + lows + prices) / 3
        return pivot.dropna().values
    
    def _cluster_price_levels(self,
                            levels: np.ndarray,
                            threshold: float = 0.02) -> np.ndarray:
        """Cluster nearby price levels"""
        from sklearn.cluster import DBSCAN
        
        # Normalize prices for clustering
        normalized_levels = levels.reshape(-1, 1) / np.mean(levels)
        
        # Perform clustering
        clustering = DBSCAN(eps=threshold, min_samples=2).fit(normalized_levels)
        
        # Get cluster centers
        unique_labels = np.unique(clustering.labels_)
        clustered_levels = []
        
        for label in unique_labels:
            if label != -1:  # Ignore noise points
                mask = clustering.labels_ == label
                clustered_levels.append(np.mean(levels[mask]))
                
        return np.array(clustered_levels)
    
    def _calculate_level_strength(self,
                                level: float,
                                prices: np.ndarray,
                                volumes: np.ndarray,
                                touches: int) -> float:
        """Calculate the strength of a price level"""
        
        # Factors that contribute to level strength:
        # 1. Number of touches (normalized)
        touch_score = min(touches / 10, 1.0)
        
        # 2. Volume at touches
        level_volume = self._calculate_level_volume(prices, volumes, level)
        volume_score = min(level_volume / np.mean(volumes), 1.0)
        
        # 3. Recency of touches
        recency_score = self._calculate_recency_score(prices, level)
        
        # 4. Price reaction magnitude
        reaction_score = self._calculate_reaction_score(prices, level)
        
        # Combine scores with weights
        weights = {
            'touches': 0.3,
            'volume': 0.3,
            'recency': 0.2,
            'reaction': 0.2
        }
        
        return (
            weights['touches'] * touch_score +
            weights['volume'] * volume_score +
            weights['recency'] * recency_score +
            weights['reaction'] * reaction_score
        )
    
    def _calculate_recency_score(self,
                               prices: np.ndarray,
                               level: float) -> float:
        """Calculate score based on how recently the level was touched"""
        touches = self._find_level_touches(prices, level)
        if len(touches) == 0:
            return 0
        
        last_touch_idx = touches[-1]
        distance_from_end = len(prices) - last_touch_idx
        
        return np.exp(-distance_from_end / len(prices))
    
    def _calculate_reaction_score(self,
                                prices: np.ndarray,
                                level: float) -> float:
        """Calculate score based on price reactions at the level"""
        touches = self._find_level_touches(prices, level)
        if len(touches) < 2:
            return 0
        
        reactions = []
        for touch_idx in touches:
            if touch_idx + 1 < len(prices):
                reaction_pct = abs(prices[touch_idx + 1] - prices[touch_idx]) / prices[touch_idx]
                reactions.append(reaction_pct)
        
        return min(np.mean(reactions) * 100, 1.0)

    def _get_price_data(self,
                       coin_id: str,
                       lookback_days: int) -> Optional[pd.DataFrame]:
        """Get price data with caching"""
        cache_key = f"{coin_id}_{lookback_days}"
        
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]
        
        # Fetch from database
        data = self.db.get_historical_data(
            coin_id=coin_id,
            start_date=datetime.now() - timedelta(days=lookback_days)
        )
        
        if data is None or len(data) == 0:
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        df.set_index('timestamp', inplace=True)
        
        # Cache the result
        self.price_cache[cache_key] = df
        
        return df