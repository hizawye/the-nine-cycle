"""
Main data collection orchestrator for Nine Cycle project.
Coordinates collection from all data sources.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from .collectors.base_collector import CollectedEvent
from .collectors.wikipedia_collector import collect_wikipedia_events
from .collectors.economic_collector import collect_economic_events
from .collectors.news_collector import collect_news_events
from .utils.config import settings
from .utils.database import get_database_manager, init_database
from .utils.logging_config import get_logger
from .utils.data_validation import DataValidator

logger = get_logger(__name__)

class DataCollectionOrchestrator:
    """Orchestrates data collection from multiple sources."""
    
    def __init__(self):
        """Initialize data collection orchestrator."""
        self.db_manager = get_database_manager()
        self.validator = DataValidator()
        self.collection_results = []
        
        # Data source collectors
        self.collectors = {
            'wikipedia': collect_wikipedia_events,
            'economic': collect_economic_events,
            'news': collect_news_events
        }
        
        # Collection priorities (order of execution)
        self.collection_order = ['wikipedia', 'economic', 'news']
    
    async def collect_all_data(
        self,
        start_year: int,
        end_year: int,
        sources: Optional[List[str]] = None,
        batch_size: int = 50,
        save_to_db: bool = True,
        validate_data: bool = True
    ) -> Dict[str, Any]:
        """
        Collect data from all specified sources.
        
        Args:
            start_year: Starting year for collection
            end_year: Ending year for collection
            sources: List of sources to collect from (default: all)
            batch_size: Number of years to process in each batch
            save_to_db: Whether to save to database
            validate_data: Whether to validate collected data
            
        Returns:
            Collection summary results
        """
        start_time = datetime.now()
        logger.info(f"Starting data collection for years {start_year}-{end_year}")
        
        # Initialize database if needed
        if save_to_db and not self.db_manager.connected:
            init_database()
        
        # Determine sources to use
        if sources is None:
            sources = self.collection_order
        else:
            # Ensure sources are in priority order
            sources = [s for s in self.collection_order if s in sources]
        
        # Collect data by batches to manage memory and processing
        total_years = end_year - start_year + 1
        batches = [
            (year, min(year + batch_size - 1, end_year))
            for year in range(start_year, end_year + 1, batch_size)
        ]
        
        collection_summary = {
            'start_year': start_year,
            'end_year': end_year,
            'total_years': total_years,
            'batches_processed': 0,
            'sources_used': sources,
            'results_by_source': {},
            'total_events_collected': 0,
            'total_events_saved': 0,
            'total_errors': 0,
            'validation_results': None,
            'duration_seconds': 0,
            'success': True
        }
        
        try:
            # Process each batch
            for batch_num, (batch_start, batch_end) in enumerate(batches):
                logger.info(f"Processing batch {batch_num + 1}/{len(batches)}: {batch_start}-{batch_end}")
                
                batch_results = await self.collect_batch(
                    batch_start, batch_end, sources, save_to_db
                )
                
                # Aggregate results
                for source, result in batch_results.items():
                    if source not in collection_summary['results_by_source']:
                        collection_summary['results_by_source'][source] = {
                            'events_collected': 0,
                            'events_saved': 0,
                            'errors': 0,
                            'batches': []
                        }
                    
                    source_summary = collection_summary['results_by_source'][source]
                    source_summary['events_collected'] += result.get('events_collected', 0)
                    source_summary['events_saved'] += result.get('events_saved', 0)
                    source_summary['errors'] += result.get('errors', 0)
                    source_summary['batches'].append(result)
                
                collection_summary['batches_processed'] += 1
            
            # Calculate totals
            for source_data in collection_summary['results_by_source'].values():
                collection_summary['total_events_collected'] += source_data['events_collected']
                collection_summary['total_events_saved'] += source_data['events_saved']
                collection_summary['total_errors'] += source_data['errors']
            
            # Validate collected data if requested
            if validate_data and save_to_db:
                logger.info("Validating collected data...")
                collection_summary['validation_results'] = self.validator.generate_validation_report()
            
            # Calculate duration
            collection_summary['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Data collection completed successfully in {collection_summary['duration_seconds']:.2f}s")
            logger.info(f"Total events collected: {collection_summary['total_events_collected']}")
            
            return collection_summary
        
        except Exception as e:
            collection_summary['success'] = False
            collection_summary['error_message'] = str(e)
            collection_summary['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            
            logger.error(f"Data collection failed: {str(e)}")
            return collection_summary
    
    async def collect_batch(
        self,
        start_year: int,
        end_year: int,
        sources: List[str],
        save_to_db: bool
    ) -> Dict[str, Any]:
        """Collect data for a specific year range batch."""
        batch_results = {}
        
        for source in sources:
            if source in self.collectors:
                try:
                    logger.info(f"Collecting from {source} for years {start_year}-{end_year}")
                    
                    # Run collection for this source
                    result = await self.collectors[source](
                        start_year, end_year, save_to_db
                    )
                    
                    batch_results[source] = result
                    
                    if result.get('success', False):
                        logger.info(f"Successfully collected {result.get('events_collected', 0)} events from {source}")
                    else:
                        logger.warning(f"Collection from {source} completed with errors: {result.get('error_message', 'Unknown error')}")
                
                except Exception as e:
                    logger.error(f"Error collecting from {source}: {str(e)}")
                    batch_results[source] = {
                        'source': source,
                        'start_year': start_year,
                        'end_year': end_year,
                        'events_collected': 0,
                        'events_saved': 0,
                        'errors': 1,
                        'success': False,
                        'error_message': str(e)
                    }
            else:
                logger.warning(f"Unknown data source: {source}")
        
        return batch_results
    
    async def collect_recent_data(self, days_back: int = 30) -> Dict[str, Any]:
        """Collect recent data (useful for ongoing updates)."""
        current_year = datetime.now().year
        start_year = current_year - 1  # Last year and current year
        
        logger.info(f"Collecting recent data from {start_year} to {current_year}")
        
        return await self.collect_all_data(
            start_year=start_year,
            end_year=current_year,
            sources=['news', 'economic'],  # Focus on recent data sources
            batch_size=1,
            save_to_db=True,
            validate_data=False  # Skip validation for recent updates
        )
    
    def get_collection_statistics(self) -> Dict[str, Any]:
        """Get collection statistics from database."""
        if not self.db_manager.connected:
            return {'error': 'Database not connected'}
        
        stats = self.db_manager.get_collection_stats()
        
        # Add additional statistics
        with self.db_manager.get_session() as session:
            # Events by source
            source_stats = session.execute("""
                SELECT source, COUNT(*) as count, MIN(year) as min_year, MAX(year) as max_year
                FROM historical_events 
                GROUP BY source 
                ORDER BY count DESC
            """).fetchall()
            
            # Events by year (last 20 years)
            current_year = datetime.now().year
            recent_years = session.execute("""
                SELECT year, COUNT(*) as count
                FROM historical_events 
                WHERE year >= %s
                GROUP BY year 
                ORDER BY year DESC
            """, (current_year - 20,)).fetchall()
            
            # Events by category
            category_stats = session.execute("""
                SELECT category, COUNT(*) as count, AVG(severity) as avg_severity
                FROM historical_events 
                GROUP BY category 
                ORDER BY count DESC
            """).fetchall()
        
        return {
            'overall_stats': stats,
            'source_breakdown': [
                {
                    'source': row[0],
                    'count': row[1],
                    'min_year': row[2],
                    'max_year': row[3],
                    'year_range': row[3] - row[2] + 1 if row[2] and row[3] else 0
                }
                for row in source_stats
            ],
            'recent_years': [
                {'year': row[0], 'count': row[1]}
                for row in recent_years
            ],
            'category_breakdown': [
                {
                    'category': row[0],
                    'count': row[1],
                    'avg_severity': round(row[2], 2) if row[2] else 0
                }
                for row in category_stats
            ]
        }
    
    def export_collection_report(self, output_file: str) -> Dict[str, Any]:
        """Export comprehensive collection report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'database_status': 'connected' if self.db_manager.connected else 'disconnected',
            'statistics': self.get_collection_statistics(),
            'validation': self.validator.generate_validation_report()
        }
        
        # Save report
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        import json
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Collection report exported to {output_path}")
        return report

# Async convenience functions for external use
async def collect_historical_data(start_year: int = 1, end_year: int = 2025) -> Dict[str, Any]:
    """
    Collect comprehensive historical data.
    
    Args:
        start_year: Starting year (default: 1 AD)
        end_year: Ending year (default: 2025)
        
    Returns:
        Collection results summary
    """
    orchestrator = DataCollectionOrchestrator()
    return await orchestrator.collect_all_data(start_year, end_year)

async def collect_sample_data(sample_years: int = 100) -> Dict[str, Any]:
    """
    Collect sample data for testing and development.
    
    Args:
        sample_years: Number of years to sample (default: 100)
        
    Returns:
        Collection results summary
    """
    # Sample recent years for better data availability
    current_year = datetime.now().year
    start_year = current_year - sample_years
    
    orchestrator = DataCollectionOrchestrator()
    return await orchestrator.collect_all_data(
        start_year=start_year,
        end_year=current_year,
        batch_size=10
    )

def get_collection_status() -> Dict[str, Any]:
    """Get current collection status and statistics."""
    orchestrator = DataCollectionOrchestrator()
    return orchestrator.get_collection_statistics()
