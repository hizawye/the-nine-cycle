"""
Base collector class for Nine Cycle project data collection.
Provides common functionality for all data collectors.
"""

import time
import logging
import requests
import asyncio
try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    aiohttp = None
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Iterator, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import json
import hashlib
from pathlib import Path

from ..utils.config import settings, EVENT_SEVERITY_LEVELS, HISTORICAL_CATEGORIES
from ..utils.database import get_database_manager
from ..utils.logging_config import DataCollectionLogger

@dataclass
class CollectedEvent:
    """Data class for collected historical events."""
    year: int
    title: str
    description: str
    category: str
    source: str
    date: Optional[datetime] = None
    subcategory: Optional[str] = None
    severity: Optional[int] = None
    source_url: Optional[str] = None
    location: Optional[str] = None
    participants: Optional[str] = None
    tags: Optional[List[str]] = None
    impact_score: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Calculate digital root after initialization."""
        self.digital_root = self.calculate_digital_root(self.year)
        
        # Estimate severity if not provided
        if self.severity is None:
            self.severity = self.estimate_severity()
    
    def calculate_digital_root(self, year: int) -> int:
        """Calculate digital root of the year."""
        while year > 9:
            year = sum(int(digit) for digit in str(year))
        return year
    
    def estimate_severity(self) -> int:
        """Estimate event severity based on description and title."""
        text = f"{self.title} {self.description}".lower()
        
        # Global impact keywords
        global_keywords = ['world war', 'global', 'worldwide', 'international', 'pandemic', 'depression']
        continental_keywords = ['european', 'asian', 'african', 'american continent', 'continent']
        national_keywords = ['national', 'country', 'nation', 'federal']
        
        if any(keyword in text for keyword in global_keywords):
            return EVENT_SEVERITY_LEVELS['global']
        elif any(keyword in text for keyword in continental_keywords):
            return EVENT_SEVERITY_LEVELS['continental']
        elif any(keyword in text for keyword in national_keywords):
            return EVENT_SEVERITY_LEVELS['national']
        else:
            return EVENT_SEVERITY_LEVELS['regional']
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        # Manually create dict to include properties and init=False fields
        data = {
            'year': self.year,
            'title': self.title,
            'description': self.description,
            'category': self.category,
            'source': self.source,
            'date': self.date,
            'subcategory': self.subcategory,
            'severity': self.severity,
            'digital_root': self.digital_root,
            'source_url': self.source_url,
            'location': self.location,
            'participants': self.participants,
            'tags': self.tags,
            'impact_score': self.impact_score,
        }

        # Convert tags list to JSON string
        if self.tags:
            data['tags'] = json.dumps(self.tags)
        
        # Convert metadata to JSON string and rename the field
        if self.metadata:
            data['collection_metadata'] = json.dumps(self.metadata)
        
        return data
    
    def get_hash(self) -> str:
        """Generate unique hash for deduplication."""
        hash_string = f"{self.year}_{self.title}_{self.source}"
        return hashlib.md5(hash_string.encode()).hexdigest()

class RateLimiter:
    """Rate limiter for API requests."""
    
    def __init__(self, rate_limit: float):
        """
        Initialize rate limiter.
        
        Args:
            rate_limit: Minimum seconds between requests
        """
        self.rate_limit = rate_limit
        self.last_request_time = 0
    
    async def wait(self):
        """Wait for rate limit if necessary."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit:
            wait_time = self.rate_limit - time_since_last
            await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def wait_sync(self):
        """Synchronous wait for rate limit."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit:
            wait_time = self.rate_limit - time_since_last
            time.sleep(wait_time)
        
        self.last_request_time = time.time()

class BaseCollector(ABC):
    """Abstract base class for all data collectors."""
    
    def __init__(self, source_name: str, rate_limit: float = 1.0):
        """
        Initialize base collector.
        
        Args:
            source_name: Name of the data source
            rate_limit: Rate limit in seconds between requests
        """
        self.source_name = source_name
        self.rate_limiter = RateLimiter(rate_limit)
        self.logger = DataCollectionLogger(source_name)
        self.db_manager = get_database_manager()
        self.session = requests.Session()
        self.collected_events = []
        self.errors = []
        self.start_time = None
        
        # Setup session headers
        self.session.headers.update({
            'User-Agent': 'Nine-Cycle-Research/1.0 (https://github.com/hizawye/nine-cycle)'
        })
    
    @abstractmethod
    async def collect_events(self, start_year: int, end_year: int) -> List[CollectedEvent]:
        """
        Abstract method to collect events from the data source.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection
            
        Returns:
            List of collected events
        """
        pass
    
    @abstractmethod
    def categorize_event(self, event_data: Dict[str, Any]) -> str:
        """
        Abstract method to categorize an event.
        
        Args:
            event_data: Raw event data
            
        Returns:
            Event category
        """
        pass
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text data."""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove special characters but keep basic punctuation
        import re
        text = re.sub(r'[^\w\s.,!?;:\-()]', '', text)
        
        return text.strip()
    
    def extract_year_from_text(self, text: str) -> Optional[int]:
        """Extract year from text string."""
        import re
        
        # Look for 4-digit years
        year_matches = re.findall(r'\b(1\d{3}|20\d{2})\b', text)
        
        if year_matches:
            year = int(year_matches[0])
            if 1 <= year <= 2025:
                return year
        
        return None
    
    def deduplicate_events(self, events: List[CollectedEvent]) -> List[CollectedEvent]:
        """Remove duplicate events based on hash."""
        seen_hashes = set()
        unique_events = []
        
        for event in events:
            event_hash = event.get_hash()
            if event_hash not in seen_hashes:
                seen_hashes.add(event_hash)
                unique_events.append(event)
            else:
                self.logger.log_warning(f"Duplicate event removed: {event.title[:50]}...")
        
        return unique_events
    
    def validate_event(self, event: CollectedEvent) -> bool:
        """Validate collected event data."""
        # Check required fields
        if not event.year or not event.title or not event.category:
            return False
        
        # Check year range
        if not (1 <= event.year <= 2025):
            return False
        
        # Check category is valid
        if event.category not in HISTORICAL_CATEGORIES:
            return False
        
        # Check title length
        if len(event.title) < 3 or len(event.title) > 500:
            return False
        
        return True
    
    def save_events_to_database(self, events: List[CollectedEvent]) -> Tuple[int, int]:
        """
        Save collected events to database.
        
        Returns:
            Tuple of (saved_count, error_count)
        """
        saved_count = 0
        error_count = 0
        
        for event in events:
            try:
                if self.validate_event(event):
                    event_data = event.to_dict()
                    self.db_manager.insert_historical_event(event_data)
                    saved_count += 1
                else:
                    error_count += 1
                    self.logger.log_warning(f"Invalid event skipped: {event.title[:50]}...")
            except Exception as e:
                error_count += 1
                self.logger.log_warning(f"Error saving event {event.title[:50]}...: {str(e)}")
        
        return saved_count, error_count
    
    def save_to_file(self, events: List[CollectedEvent], filename: str):
        """Save collected events to JSON file."""
        file_path = settings.DATA_RAW_PATH / self.source_name / f"{filename}.json"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        events_data = [event.to_dict() for event in events]
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(events_data, f, indent=2, default=str, ensure_ascii=False)
        
        self.logger.logger.info(f"Events saved to file: {file_path}")
    
    async def make_request(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request with rate limiting and error handling.
        
        Args:
            url: Request URL
            params: Query parameters
            headers: Additional headers
            
        Returns:
            Response data as dictionary or None if error
        """
        await self.rate_limiter.wait()
        
        if not HAS_AIOHTTP:
            # Fallback to synchronous request
            return self.make_request_sync(url, params, headers)
        
        try:
            request_headers = self.session.headers.copy()
            if headers:
                request_headers.update(headers)
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=settings.TIMEOUT_SECONDS)) as session:
                async with session.get(url, params=params, headers=request_headers) as response:
                    if response.status == 200:
                        if 'application/json' in response.headers.get('content-type', ''):
                            return await response.json()
                        else:
                            text = await response.text()
                            return {'text': text}
                    else:
                        self.logger.log_warning(f"HTTP {response.status} for URL: {url}")
                        return None
        
        except asyncio.TimeoutError:
            self.logger.log_warning(f"Timeout for URL: {url}")
            return None
        except Exception as e:
            self.logger.log_warning(f"Request error for URL {url}: {str(e)}")
            return None
    
    def make_request_sync(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """
        Synchronous HTTP request with rate limiting and error handling.
        
        Args:
            url: Request URL
            params: Query parameters
            headers: Additional headers
            
        Returns:
            Response data as dictionary or None if error
        """
        self.rate_limiter.wait_sync()
        
        try:
            request_headers = self.session.headers.copy()
            if headers:
                request_headers.update(headers)
            
            response = self.session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=settings.TIMEOUT_SECONDS
            )
            
            if response.status_code == 200:
                if 'application/json' in response.headers.get('content-type', ''):
                    return response.json()
                else:
                    return {'text': response.text}
            else:
                self.logger.log_warning(f"HTTP {response.status_code} for URL: {url}")
                return None
        
        except requests.exceptions.Timeout:
            self.logger.log_warning(f"Timeout for URL: {url}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.log_warning(f"Request error for URL {url}: {str(e)}")
            return None
    
    async def run_collection(self, start_year: int, end_year: int, save_to_db: bool = True, save_to_file: bool = True) -> Dict[str, Any]:
        """
        Run the complete data collection process.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection
            save_to_db: Whether to save to database
            save_to_file: Whether to save to file
            
        Returns:
            Collection results summary
        """
        self.start_time = datetime.now()
        collection_type = f"{start_year}-{end_year}"
        
        self.logger.start_collection(collection_type, end_year - start_year + 1)
        
        try:
            # Collect events
            events = await self.collect_events(start_year, end_year)
            
            # Deduplicate
            events = self.deduplicate_events(events)
            
            # Save results
            saved_count = 0
            error_count = 0
            
            if save_to_db:
                saved_count, error_count = self.save_events_to_database(events)
            
            if save_to_file:
                filename = f"events_{start_year}_{end_year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                self.save_to_file(events, filename)
            
            # Calculate duration
            duration = (datetime.now() - self.start_time).total_seconds()
            
            # Log success
            self.logger.log_success(collection_type, len(events), duration)
            
            # Return summary
            return {
                'source': self.source_name,
                'start_year': start_year,
                'end_year': end_year,
                'events_collected': len(events),
                'events_saved': saved_count,
                'errors': error_count,
                'duration_seconds': duration,
                'success': True
            }
        
        except Exception as e:
            duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            self.logger.log_error(collection_type, e, len(self.collected_events))
            
            return {
                'source': self.source_name,
                'start_year': start_year,
                'end_year': end_year,
                'events_collected': len(self.collected_events),
                'events_saved': 0,
                'errors': 1,
                'duration_seconds': duration,
                'success': False,
                'error_message': str(e)
            }
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.session.close()
