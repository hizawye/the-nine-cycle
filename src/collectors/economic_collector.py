"""
Economic data collector for Nine Cycle project.
Collects economic events and indicators from various sources.
"""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

from .base_collector import BaseCollector, CollectedEvent
from ..utils.config import settings, DATA_SOURCES

class EconomicCollector(BaseCollector):
    """Collector for economic data and events."""
    
    def __init__(self):
        """Initialize economic data collector."""
        super().__init__('economic', 0.5)  # 0.5 second rate limit
        
        self.world_bank_url = DATA_SOURCES['worldbank']['base_url']
        self.world_bank_indicators = DATA_SOURCES['worldbank']['indicators']
        self.alpha_vantage_url = 'https://www.alphavantage.co/query'
        
        # Economic event indicators and thresholds
        self.crisis_indicators = {
            'gdp_decline': -2.0,      # GDP decline of 2% or more
            'inflation_spike': 10.0,   # Inflation above 10%
            'unemployment_rise': 8.0,  # Unemployment above 8%
            'market_crash': -15.0      # Stock market decline of 15% or more
        }
        
        # Major economies to track
        self.major_economies = [
            'US', 'CN', 'JP', 'DE', 'GB', 'FR', 'IN', 'IT', 'BR', 'CA',
            'RU', 'KR', 'ES', 'AU', 'MX', 'ID', 'NL', 'SA', 'TR', 'CH'
        ]
    
    async def collect_events(self, start_year: int, end_year: int) -> List[CollectedEvent]:
        """
        Collect economic events from multiple sources.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection
            
        Returns:
            List of collected economic events
        """
        all_events = []
        
        # Collect from World Bank data
        self.logger.log_progress(0, 3, "Collecting World Bank economic indicators")
        wb_events = await self.collect_world_bank_events(start_year, end_year)
        all_events.extend(wb_events)
        
        # Collect from Alpha Vantage (for more recent data)
        if end_year >= 1999:  # Alpha Vantage has data from 1999
            self.logger.log_progress(1, 3, "Collecting Alpha Vantage market data")
            av_events = await self.collect_alpha_vantage_events(start_year, end_year)
            all_events.extend(av_events)
        
        # Collect known historical economic crises
        self.logger.log_progress(2, 3, "Adding known historical economic events")
        historical_events = self.get_historical_economic_events(start_year, end_year)
        all_events.extend(historical_events)
        
        return all_events
    
    async def collect_world_bank_events(self, start_year: int, end_year: int) -> List[CollectedEvent]:
        """Collect economic events from World Bank data."""
        events = []
        
        for country in self.major_economies:
            for indicator in self.world_bank_indicators:
                # Get economic data for country and indicator
                data = await self.get_world_bank_data(country, indicator, start_year, end_year)
                
                if data:
                    # Analyze data for significant events
                    indicator_events = self.analyze_economic_data(data, country, indicator)
                    events.extend(indicator_events)
        
        return events
    
    async def get_world_bank_data(self, country: str, indicator: str, start_year: int, end_year: int) -> Optional[List[Dict[str, Any]]]:
        """Get economic data from World Bank API."""
        url = f"{self.world_bank_url}/country/{country}/indicator/{indicator}"
        params = {
            'date': f"{start_year}:{end_year}",
            'format': 'json',
            'per_page': 1000
        }
        
        response = await self.make_request(url, params)
        if response and isinstance(response, list) and len(response) > 1:
            return response[1]  # Data is in second element
        
        return None
    
    def analyze_economic_data(self, data: List[Dict[str, Any]], country: str, indicator: str) -> List[CollectedEvent]:
        """Analyze economic data to identify significant events."""
        events = []
        
        # Sort data by year
        data = sorted(data, key=lambda x: int(x.get('date', 0)))
        
        for i, record in enumerate(data):
            year = int(record.get('date', 0))
            value = record.get('value')
            
            if not value or year < 1:
                continue
            
            # Calculate year-over-year change if possible
            if i > 0:
                prev_value = data[i-1].get('value')
                if prev_value:
                    change_pct = ((value - prev_value) / prev_value) * 100
                    
                    # Check for significant economic events
                    event = self.detect_economic_event(
                        year, country, indicator, value, change_pct
                    )
                    if event:
                        events.append(event)
        
        return events
    
    def detect_economic_event(self, year: int, country: str, indicator: str, value: float, change_pct: float) -> Optional[CollectedEvent]:
        """Detect if economic data represents a significant event."""
        
        # GDP-related events
        if indicator == 'NY.GDP.MKTP.CD':  # GDP current USD
            if change_pct <= self.crisis_indicators['gdp_decline']:
                return CollectedEvent(
                    year=year,
                    title=f"Economic recession in {country}",
                    description=f"GDP declined by {abs(change_pct):.1f}% in {country}, indicating economic recession.",
                    category='economic',
                    subcategory='recession',
                    source='world_bank',
                    location=country,
                    impact_score=abs(change_pct) / 10,  # Scale impact
                    metadata={
                        'indicator': indicator,
                        'value': value,
                        'change_percent': change_pct,
                        'threshold': self.crisis_indicators['gdp_decline']
                    }
                )
        
        # Inflation-related events
        elif indicator == 'FP.CPI.TOTL.ZG':  # Inflation, consumer prices
            if value >= self.crisis_indicators['inflation_spike']:
                return CollectedEvent(
                    year=year,
                    title=f"High inflation in {country}",
                    description=f"Inflation reached {value:.1f}% in {country}, indicating economic instability.",
                    category='economic',
                    subcategory='inflation',
                    source='world_bank',
                    location=country,
                    impact_score=value / 20,  # Scale impact
                    metadata={
                        'indicator': indicator,
                        'value': value,
                        'threshold': self.crisis_indicators['inflation_spike']
                    }
                )
        
        # Unemployment-related events
        elif indicator == 'SL.UEM.TOTL.ZS':  # Unemployment rate
            if value >= self.crisis_indicators['unemployment_rise']:
                return CollectedEvent(
                    year=year,
                    title=f"High unemployment in {country}",
                    description=f"Unemployment reached {value:.1f}% in {country}, indicating economic distress.",
                    category='economic',
                    subcategory='unemployment',
                    source='world_bank',
                    location=country,
                    impact_score=value / 15,  # Scale impact
                    metadata={
                        'indicator': indicator,
                        'value': value,
                        'threshold': self.crisis_indicators['unemployment_rise']
                    }
                )
        
        return None
    
    async def collect_alpha_vantage_events(self, start_year: int, end_year: int) -> List[CollectedEvent]:
        """Collect market events from Alpha Vantage."""
        events = []
        
        if not settings.ALPHA_VANTAGE_API_KEY:
            self.logger.log_warning("Alpha Vantage API key not provided, skipping market data collection")
            return events
        
        # Get major stock indices data
        indices = ['SPX', 'DJI', 'IXIC', 'FTSE', 'N225', 'GDAXI']  # Major global indices
        
        for index in indices:
            index_events = await self.get_stock_index_events(index, start_year, end_year)
            events.extend(index_events)
        
        return events
    
    async def get_stock_index_events(self, symbol: str, start_year: int, end_year: int) -> List[CollectedEvent]:
        """Get significant events from stock index data."""
        events = []
        
        params = {
            'function': 'TIME_SERIES_MONTHLY',
            'symbol': symbol,
            'apikey': settings.ALPHA_VANTAGE_API_KEY
        }
        
        response = await self.make_request(self.alpha_vantage_url, params)
        if not response or 'Monthly Time Series' not in response:
            return events
        
        time_series = response['Monthly Time Series']
        
        # Convert to list and sort by date
        data_points = []
        for date_str, values in time_series.items():
            year = int(date_str.split('-')[0])
            if start_year <= year <= end_year:
                data_points.append({
                    'date': date_str,
                    'year': year,
                    'close': float(values['4. close'])
                })
        
        data_points.sort(key=lambda x: x['date'])
        
        # Detect market crashes (significant monthly declines)
        for i in range(1, len(data_points)):
            current = data_points[i]
            previous = data_points[i-1]
            
            change_pct = ((current['close'] - previous['close']) / previous['close']) * 100
            
            if change_pct <= self.crisis_indicators['market_crash']:
                events.append(CollectedEvent(
                    year=current['year'],
                    title=f"Stock market crash - {symbol}",
                    description=f"Stock index {symbol} declined by {abs(change_pct):.1f}% in {current['date'][:7]}.",
                    category='economic',
                    subcategory='market_crash',
                    source='alpha_vantage',
                    impact_score=abs(change_pct) / 20,
                    metadata={
                        'symbol': symbol,
                        'decline_percent': change_pct,
                        'date': current['date'],
                        'close_price': current['close']
                    }
                ))
        
        return events
    
    def get_historical_economic_events(self, start_year: int, end_year: int) -> List[CollectedEvent]:
        """Get known major historical economic events."""
        historical_events = [
            # Major economic crises and events
            (1929, "Stock Market Crash of 1929", "Black Tuesday stock market crash marked the beginning of the Great Depression", "market_crash", 5.0),
            (1930, "Great Depression begins", "Global economic depression lasting throughout the 1930s", "depression", 5.0),
            (1973, "1973 Oil Crisis", "Oil embargo by OPEC nations caused global energy crisis", "oil_crisis", 4.0),
            (1979, "1979 Energy Crisis", "Second major oil crisis due to Iranian Revolution", "oil_crisis", 3.5),
            (1987, "Black Monday", "Global stock market crash on October 19, 1987", "market_crash", 4.0),
            (1997, "Asian Financial Crisis", "Financial crisis that affected Asian economies", "financial_crisis", 4.5),
            (2000, "Dot-com Bubble Burst", "Internet company stock bubble burst", "market_crash", 3.5),
            (2008, "Global Financial Crisis", "Subprime mortgage crisis led to global recession", "financial_crisis", 5.0),
            (2010, "European Debt Crisis", "Sovereign debt crisis in European Union", "debt_crisis", 4.0),
            (2020, "COVID-19 Economic Impact", "Global economic disruption due to pandemic", "pandemic_economic", 4.5),
            
            # Other significant economic events
            (1933, "New Deal Programs", "US government economic recovery programs during Great Depression", "economic_policy", 3.0),
            (1944, "Bretton Woods Agreement", "International monetary system established", "monetary_policy", 3.5),
            (1971, "Nixon Shock", "US ended convertibility of dollar to gold", "monetary_policy", 3.5),
            (1999, "Euro Introduction", "European single currency introduced", "monetary_policy", 3.0),
            
            # Hyperinflation events
            (1923, "German Hyperinflation", "Hyperinflation in Weimar Republic Germany", "hyperinflation", 4.0),
            (1946, "Hungarian Hyperinflation", "Worst case of hyperinflation in recorded history", "hyperinflation", 4.5),
            (1980, "Israeli Hyperinflation", "Severe inflation crisis in Israel", "hyperinflation", 3.0),
            (2008, "Zimbabwe Hyperinflation", "Extreme hyperinflation in Zimbabwe", "hyperinflation", 3.5),
        ]
        
        events = []
        for year, title, description, subcategory, impact_score in historical_events:
            if start_year <= year <= end_year:
                events.append(CollectedEvent(
                    year=year,
                    title=title,
                    description=description,
                    category='economic',
                    subcategory=subcategory,
                    source='historical_database',
                    impact_score=impact_score,
                    verified=True,  # These are well-documented events
                    metadata={
                        'event_type': 'historical_major_event',
                        'confidence': 'high'
                    }
                ))
        
        return events
    
    def categorize_event(self, event_data: Dict[str, Any]) -> str:
        """Categorize an event (always economic for this collector)."""
        return 'economic'

# Async convenience function for external use
async def collect_economic_events(start_year: int, end_year: int, save_to_db: bool = True) -> Dict[str, Any]:
    """
    Convenient function to collect economic events.
    
    Args:
        start_year: Starting year for collection
        end_year: Ending year for collection
        save_to_db: Whether to save to database
        
    Returns:
        Collection results summary
    """
    collector = EconomicCollector()
    return await collector.run_collection(start_year, end_year, save_to_db=save_to_db)
