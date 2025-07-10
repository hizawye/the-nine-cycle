"""
News data collector for Nine Cycle project.
Collects recent historical events from news sources.
"""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import re

from .base_collector import BaseCollector, CollectedEvent
from ..utils.config import settings, DATA_SOURCES

class NewsCollector(BaseCollector):
    """Collector for news data and recent events."""
    
    def __init__(self):
        """Initialize news data collector."""
        super().__init__('news', DATA_SOURCES['newsapi']['rate_limit'])
        
        self.news_api_url = DATA_SOURCES['newsapi']['base_url']
        
        # Keywords for different event categories
        self.search_keywords = {
            'economic': [
                'recession', 'economic crisis', 'financial crisis', 'market crash',
                'inflation', 'unemployment', 'economic boom', 'trade war'
            ],
            'political': [
                'election', 'war', 'conflict', 'treaty', 'coup', 'revolution',
                'political crisis', 'sanctions', 'diplomatic'
            ],
            'technological': [
                'breakthrough', 'innovation', 'discovery', 'invention',
                'technology', 'artificial intelligence', 'space exploration'
            ],
            'social': [
                'social movement', 'protest', 'civil rights', 'cultural change',
                'demographic shift', 'social reform'
            ],
            'environmental': [
                'natural disaster', 'climate change', 'pandemic', 'epidemic',
                'environmental crisis', 'earthquake', 'hurricane'
            ]
        }
        
        # News sources that provide historical coverage
        self.historical_sources = [
            'bbc-news', 'cnn', 'the-new-york-times', 'the-guardian-uk',
            'reuters', 'associated-press', 'time', 'national-geographic'
        ]
    
    async def collect_events(self, start_year: int, end_year: int) -> List[CollectedEvent]:
        """
        Collect events from news sources.
        Note: Most news APIs only provide recent data (last few years).
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection
            
        Returns:
            List of collected news events
        """
        all_events = []
        
        if not settings.NEWS_API_KEY:
            self.logger.log_warning("News API key not provided, skipping news data collection")
            return all_events
        
        # News APIs typically only have data from recent years
        current_year = datetime.now().year
        effective_start_year = max(start_year, current_year - 5)  # Last 5 years
        
        if effective_start_year > end_year:
            self.logger.log_warning(f"News API only covers recent years, no data for {start_year}-{end_year}")
            return all_events
        
        # Collect events by category and keywords
        for category, keywords in self.search_keywords.items():
            self.logger.log_progress(
                list(self.search_keywords.keys()).index(category) + 1,
                len(self.search_keywords),
                f"Collecting {category} news events"
            )
            
            category_events = await self.collect_category_events(
                category, keywords, effective_start_year, end_year
            )
            all_events.extend(category_events)
        
        return all_events
    
    async def collect_category_events(self, category: str, keywords: List[str], start_year: int, end_year: int) -> List[CollectedEvent]:
        """Collect events for a specific category."""
        events = []
        
        for keyword in keywords:
            # Search for articles with this keyword
            articles = await self.search_news_articles(keyword, start_year, end_year)
            
            for article in articles:
                event = self.convert_article_to_event(article, category)
                if event:
                    events.append(event)
        
        return events
    
    async def search_news_articles(self, query: str, start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """Search for news articles using News API."""
        articles = []
        
        # Calculate date range
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"
        
        # Limit end date to what API supports
        max_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date > max_date:
            end_date = max_date
        
        # Search everything endpoint for historical data
        params = {
            'q': query,
            'from': start_date,
            'to': end_date,
            'sortBy': 'relevancy',
            'pageSize': 50,  # Max per request
            'apiKey': settings.NEWS_API_KEY
        }
        
        response = await self.make_request(f"{self.news_api_url}/everything", params)
        
        if response and 'articles' in response:
            articles.extend(response['articles'])
        
        return articles
    
    def convert_article_to_event(self, article: Dict[str, Any], category: str) -> Optional[CollectedEvent]:
        """Convert a news article to a historical event."""
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        url = article.get('url', '')
        published_at = article.get('publishedAt', '')
        source_name = article.get('source', {}).get('name', 'unknown')
        
        if not title or not published_at:
            return None
        
        # Extract year from publication date
        try:
            pub_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            year = pub_date.year
        except:
            return None
        
        # Clean and combine description
        full_description = f"{description} {content}".strip()
        if not full_description:
            full_description = title
        
        # Extract additional information
        location = self.extract_location_from_article(title, full_description)
        participants = self.extract_participants_from_article(title, full_description)
        severity = self.estimate_event_severity(title, full_description, category)
        
        return CollectedEvent(
            year=year,
            date=pub_date,
            title=self.clean_text(title),
            description=self.clean_text(full_description),
            category=category,
            source='news_api',
            source_url=url,
            location=location,
            participants=participants,
            severity=severity,
            metadata={
                'news_source': source_name,
                'published_at': published_at,
                'collection_method': 'news_api_search'
            }
        )
    
    def extract_location_from_article(self, title: str, content: str) -> Optional[str]:
        """Extract location information from article text."""
        text = f"{title} {content}".lower()
        
        # Common location patterns in news
        location_patterns = [
            r'\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:government|president|prime minister)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:capital|city|country)',
        ]
        
        # List of known countries and major cities
        known_locations = [
            'United States', 'China', 'Japan', 'Germany', 'United Kingdom', 'France',
            'India', 'Italy', 'Brazil', 'Canada', 'Russia', 'South Korea', 'Spain',
            'Australia', 'Mexico', 'Indonesia', 'Netherlands', 'Saudi Arabia', 'Turkey',
            'Switzerland', 'New York', 'London', 'Tokyo', 'Paris', 'Berlin', 'Moscow'
        ]
        
        for location in known_locations:
            if location.lower() in text:
                return location
        
        # Try pattern matching as fallback
        for pattern in location_patterns:
            match = re.search(pattern, title + ' ' + content)
            if match:
                location = match.group(1)
                if len(location) > 2 and location.istitle():
                    return location
        
        return None
    
    def extract_participants_from_article(self, title: str, content: str) -> Optional[str]:
        """Extract key participants/actors from article text."""
        text = f"{title} {content}"
        
        # Look for person names (capitalized words)
        person_pattern = r'\b([A-Z][a-z]+\s+[A-Z][a-z]+)\b'
        person_matches = re.findall(person_pattern, text)
        
        # Filter out common non-person phrases
        non_persons = [
            'United States', 'White House', 'World Bank', 'European Union',
            'United Nations', 'News Corp', 'Associated Press', 'New York Times'
        ]
        
        participants = []
        for match in person_matches:
            if match not in non_persons and len(participants) < 3:
                participants.append(match)
        
        if participants:
            return ', '.join(participants)
        
        return None
    
    def estimate_event_severity(self, title: str, content: str, category: str) -> int:
        """Estimate event severity based on article content."""
        text = f"{title} {content}".lower()
        
        # Severity keywords
        high_severity_keywords = [
            'crisis', 'disaster', 'catastrophe', 'emergency', 'collapse',
            'war', 'conflict', 'pandemic', 'recession', 'crash'
        ]
        
        medium_severity_keywords = [
            'significant', 'major', 'important', 'breakthrough', 'milestone',
            'historic', 'unprecedented', 'massive'
        ]
        
        # Check for high severity indicators
        for keyword in high_severity_keywords:
            if keyword in text:
                return 4  # High severity
        
        # Check for medium severity indicators
        for keyword in medium_severity_keywords:
            if keyword in text:
                return 3  # Medium severity
        
        return 2  # Default to regional/medium-low severity
    
    def categorize_event(self, event_data: Dict[str, Any]) -> str:
        """Categorize an event based on its content."""
        title = event_data.get('title', '').lower()
        content = event_data.get('content', '').lower()
        text = f"{title} {content}"
        
        # Score each category based on keyword matches
        category_scores = {}
        
        for category, keywords in self.search_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword.lower() in text:
                    score += 1
            category_scores[category] = score
        
        # Return category with highest score
        if category_scores:
            best_category = max(category_scores, key=category_scores.get)
            if category_scores[best_category] > 0:
                return best_category
        
        return 'social'  # Default category for news events

# Async convenience function for external use
async def collect_news_events(start_year: int, end_year: int, save_to_db: bool = True) -> Dict[str, Any]:
    """
    Convenient function to collect news events.
    
    Args:
        start_year: Starting year for collection
        end_year: Ending year for collection
        save_to_db: Whether to save to database
        
    Returns:
        Collection results summary
    """
    collector = NewsCollector()
    return await collector.run_collection(start_year, end_year, save_to_db=save_to_db)
