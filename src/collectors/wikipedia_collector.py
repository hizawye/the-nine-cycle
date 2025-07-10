"""
Wikipedia data collector for Nine Cycle project.
Collects historical events from Wikipedia.
"""

import asyncio
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import urljoin, quote
import json

from .base_collector import BaseCollector, CollectedEvent
from ..utils.config import settings, HISTORICAL_CATEGORIES, DATA_SOURCES

class WikipediaCollector(BaseCollector):
    """Collector for Wikipedia historical events."""
    
    def __init__(self):
        """Initialize Wikipedia collector."""
        wiki_config = DATA_SOURCES['wikipedia']
        super().__init__('wikipedia', wiki_config['rate_limit'])
        
        self.api_url = wiki_config['api_url']
        self.base_url = wiki_config['base_url']
        
        # Wikipedia categories to search for historical events
        self.search_categories = {
            'economic': [
                'Economic crises', 'Stock market crashes', 'Recessions',
                'Financial crises', 'Economic history', 'Banking crises'
            ],
            'political': [
                'Wars', 'Revolutions', 'Political history', 'Conflicts',
                'Treaties', 'Elections', 'Coups d\'état', 'Civil wars'
            ],
            'technological': [
                'Inventions', 'Scientific discoveries', 'Technological history',
                'Industrial Revolution', 'Space exploration', 'Medical breakthroughs'
            ],
            'social': [
                'Social movements', 'Civil rights', 'Cultural history',
                'Demographic history', 'Educational history', 'Religious history'
            ],
            'environmental': [
                'Natural disasters', 'Pandemics', 'Climate history',
                'Environmental history', 'Geological events', 'Extinctions'
            ]
        }
        
        # Wikipedia year pages pattern
        self.year_pages = [
            'AD_{}', '{}', '{}_AD', '{}_CE'
        ]
    
    async def collect_events(self, start_year: int, end_year: int) -> List[CollectedEvent]:
        """
        Collect historical events from Wikipedia.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection
            
        Returns:
            List of collected events
        """
        all_events = []
        
        for year in range(start_year, end_year + 1):
            self.logger.log_progress(year - start_year + 1, end_year - start_year + 1, f"Collecting year {year}")
            
            # Collect from year pages
            year_events = await self.collect_events_for_year(year)
            all_events.extend(year_events)
            
            # Collect from category searches (sample years to avoid overload)
            if year % 10 == 0:  # Every 10 years
                category_events = await self.collect_events_from_categories(year)
                all_events.extend(category_events)
        
        return all_events
    
    async def collect_events_for_year(self, year: int) -> List[CollectedEvent]:
        """Collect events for a specific year from Wikipedia year pages."""
        events = []
        
        for page_pattern in self.year_pages:
            page_title = page_pattern.format(year)
            
            # Get page content
            page_data = await self.get_wikipedia_page(page_title)
            if not page_data:
                continue
            
            # Extract events from page content
            page_events = self.extract_events_from_page(page_data, year)
            events.extend(page_events)
        
        return events
    
    async def collect_events_from_categories(self, around_year: int) -> List[CollectedEvent]:
        """Collect events from Wikipedia categories around a specific year."""
        events = []
        
        for category, subcategories in self.search_categories.items():
            for subcategory in subcategories:
                # Search for pages in this category
                search_results = await self.search_wikipedia_category(subcategory, around_year)
                
                for result in search_results:
                    page_data = await self.get_wikipedia_page(result['title'])
                    if page_data:
                        # Extract year from page content
                        extracted_year = self.extract_year_from_page(page_data, around_year)
                        if extracted_year:
                            page_events = self.extract_events_from_page(page_data, extracted_year, category)
                            events.extend(page_events)
        
        return events
    
    async def get_wikipedia_page(self, page_title: str) -> Optional[Dict[str, Any]]:
        """Get Wikipedia page content via API."""
        params = {
            'action': 'query',
            'format': 'json',
            'titles': page_title,
            'prop': 'extracts|info',
            'exintro': '1',
            'explaintext': '1',
            'inprop': 'url'
        }
        
        response = await self.make_request(self.api_url, params)
        if not response:
            return None
        
        pages = response.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            if page_id != '-1':  # Page exists
                return page_data
        
        return None
    
    async def search_wikipedia_category(self, category: str, year_filter: int = None) -> List[Dict[str, Any]]:
        """Search Wikipedia for pages in a specific category."""
        params = {
            'action': 'query',
            'format': 'json',
            'list': 'search',
            'srsearch': f'incategory:"{category}"',
            'srlimit': 20,
            'srprop': 'title|snippet'
        }
        
        if year_filter:
            # Add year to search to filter results
            params['srsearch'] += f' {year_filter}'
        
        response = await self.make_request(self.api_url, params)
        if not response:
            return []
        
        return response.get('query', {}).get('search', [])
    
    def extract_events_from_page(self, page_data: Dict[str, Any], year: int, category: str = None) -> List[CollectedEvent]:
        """Extract historical events from Wikipedia page content."""
        events = []
        
        title = page_data.get('title', '')
        content = page_data.get('extract', '')
        page_url = page_data.get('fullurl', '')
        
        if not content:
            return events
        
        # Split content into sections
        sections = self.split_content_into_sections(content)
        
        for section_title, section_content in sections:
            # Determine category from section title if not provided
            event_category = category or self.categorize_event({'title': section_title, 'content': section_content})
            
            # Extract individual events from section
            section_events = self.extract_events_from_section(section_content, year, event_category)
            
            for event_data in section_events:
                event = CollectedEvent(
                    year=year,
                    title=event_data['title'],
                    description=event_data['description'],
                    category=event_category,
                    source='wikipedia',
                    source_url=page_url,
                    location=event_data.get('location'),
                    participants=event_data.get('participants'),
                    tags=event_data.get('tags', []),
                    metadata={
                        'wikipedia_page': title,
                        'section': section_title,
                        'extraction_method': 'page_parsing'
                    }
                )
                
                events.append(event)
        
        return events
    
    def split_content_into_sections(self, content: str) -> List[tuple]:
        """Split Wikipedia page content into sections."""
        sections = []
        
        # Look for section headers (marked by == in Wikipedia markup)
        section_pattern = r'=+\s*([^=]+?)\s*=+'
        section_matches = list(re.finditer(section_pattern, content))
        
        if not section_matches:
            # No sections found, treat entire content as one section
            return [('Main content', content)]
        
        for i, match in enumerate(section_matches):
            section_title = match.group(1).strip()
            section_start = match.end()
            
            # Find section end (next section or end of content)
            if i + 1 < len(section_matches):
                section_end = section_matches[i + 1].start()
            else:
                section_end = len(content)
            
            section_content = content[section_start:section_end].strip()
            if section_content:
                sections.append((section_title, section_content))
        
        return sections
    
    def extract_events_from_section(self, content: str, year: int, category: str) -> List[Dict[str, Any]]:
        """Extract individual events from a content section."""
        events = []
        
        # Split content into sentences/bullet points
        sentences = self.split_into_events(content)
        
        for sentence in sentences:
            if len(sentence.strip()) < 20:  # Skip very short sentences
                continue
            
            # Extract event details
            event_data = self.parse_event_sentence(sentence, year, category)
            if event_data:
                events.append(event_data)
        
        return events
    
    def split_into_events(self, content: str) -> List[str]:
        """Split content into individual event sentences."""
        # Split on common delimiters
        events = []
        
        # Split on bullet points, line breaks, and sentence endings
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Remove bullet point markers
            line = re.sub(r'^[•\-\*]\s*', '', line)
            
            # Split long lines on sentence boundaries
            sentences = re.split(r'[.!?]+\s+(?=[A-Z])', line)
            
            for sentence in sentences:
                sentence = sentence.strip()
                if len(sentence) > 20:  # Minimum length for meaningful event
                    events.append(sentence)
        
        return events
    
    def parse_event_sentence(self, sentence: str, year: int, category: str) -> Optional[Dict[str, Any]]:
        """Parse an event sentence to extract structured data."""
        # Clean the sentence
        sentence = self.clean_text(sentence)
        
        if not sentence:
            return None
        
        # Extract key information
        title = self.extract_event_title(sentence)
        description = sentence
        location = self.extract_location(sentence)
        participants = self.extract_participants(sentence)
        tags = self.extract_tags(sentence, category)
        
        return {
            'title': title,
            'description': description,
            'location': location,
            'participants': participants,
            'tags': tags
        }
    
    def extract_event_title(self, sentence: str) -> str:
        """Extract a concise title from event sentence."""
        # Take first part of sentence up to first comma or 50 characters
        title = sentence.split(',')[0]
        if len(title) > 50:
            title = title[:50].rsplit(' ', 1)[0] + '...'
        
        return title.strip()
    
    def extract_location(self, sentence: str) -> Optional[str]:
        """Extract location information from sentence."""
        # Look for common location patterns
        location_patterns = [
            r'\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
            r'\bat\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:is|was|becomes?)',
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, sentence)
            if match:
                location = match.group(1)
                # Filter out common non-location words
                non_locations = ['January', 'February', 'March', 'April', 'May', 'June',
                               'July', 'August', 'September', 'October', 'November', 'December',
                               'The', 'This', 'That', 'It', 'He', 'She', 'They']
                if location not in non_locations:
                    return location
        
        return None
    
    def extract_participants(self, sentence: str) -> Optional[str]:
        """Extract participants/actors from sentence."""
        # Look for people, organizations, countries
        participant_patterns = [
            r'([A-Z][a-z]+\s+[A-Z][a-z]+)',  # Person names
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Empire|Kingdom|Republic|Nation|Army|Forces?))',
        ]
        
        participants = []
        for pattern in participant_patterns:
            matches = re.findall(pattern, sentence)
            participants.extend(matches)
        
        if participants:
            return ', '.join(list(set(participants))[:3])  # Limit to 3 participants
        
        return None
    
    def extract_tags(self, sentence: str, category: str) -> List[str]:
        """Extract relevant tags from sentence."""
        tags = [category]
        
        # Category-specific keywords
        category_keywords = {
            'economic': ['crisis', 'crash', 'recession', 'boom', 'inflation', 'deflation', 'trade', 'market'],
            'political': ['war', 'battle', 'treaty', 'election', 'coup', 'revolution', 'independence', 'alliance'],
            'technological': ['invention', 'discovery', 'patent', 'innovation', 'breakthrough', 'development'],
            'social': ['movement', 'rights', 'protest', 'reform', 'migration', 'culture', 'education'],
            'environmental': ['disaster', 'earthquake', 'flood', 'pandemic', 'epidemic', 'climate', 'extinction']
        }
        
        sentence_lower = sentence.lower()
        if category in category_keywords:
            for keyword in category_keywords[category]:
                if keyword in sentence_lower:
                    tags.append(keyword)
        
        return list(set(tags))  # Remove duplicates
    
    def extract_year_from_page(self, page_data: Dict[str, Any], around_year: int) -> Optional[int]:
        """Extract the most relevant year from page content."""
        content = page_data.get('extract', '')
        title = page_data.get('title', '')
        
        # Look for years in title first
        year = self.extract_year_from_text(title)
        if year and abs(year - around_year) <= 50:  # Within 50 years
            return year
        
        # Look for years in content
        years = re.findall(r'\b(1\d{3}|20\d{2})\b', content)
        if years:
            # Find year closest to around_year
            valid_years = [int(y) for y in years if 1 <= int(y) <= 2025]
            if valid_years:
                closest_year = min(valid_years, key=lambda y: abs(y - around_year))
                if abs(closest_year - around_year) <= 50:
                    return closest_year
        
        return None
    
    def categorize_event(self, event_data: Dict[str, Any]) -> str:
        """Categorize an event based on its content."""
        title = event_data.get('title', '').lower()
        content = event_data.get('content', '').lower()
        text = f"{title} {content}"
        
        # Category keywords with weights
        category_scores = {}
        
        for category, keywords in HISTORICAL_CATEGORIES.items():
            score = 0
            for keyword in keywords:
                if keyword.lower() in text:
                    score += 1
            category_scores[category] = score
        
        # Return category with highest score, default to 'political'
        if category_scores:
            best_category = max(category_scores, key=category_scores.get)
            if category_scores[best_category] > 0:
                return best_category
        
        return 'political'  # Default category

# Async convenience function for external use
async def collect_wikipedia_events(start_year: int, end_year: int, save_to_db: bool = True) -> Dict[str, Any]:
    """
    Convenient function to collect Wikipedia events.
    
    Args:
        start_year: Starting year for collection
        end_year: Ending year for collection
        save_to_db: Whether to save to database
        
    Returns:
        Collection results summary
    """
    collector = WikipediaCollector()
    return await collector.run_collection(start_year, end_year, save_to_db=save_to_db)
