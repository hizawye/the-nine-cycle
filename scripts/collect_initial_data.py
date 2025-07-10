"""
Initial data collection script for Nine Cycle project.
Collects sample data to get started.
"""

import sys
import asyncio
import logging
from pathlib import Path

# Add src to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.data_collection import collect_sample_data, collect_historical_data
from src.utils.logging_config import setup_logging
from src.utils.database import test_database_connection

async def main():
    """Run initial data collection."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting initial data collection for Nine Cycle project...")
    
    # Test database connection
    if not test_database_connection():
        logger.error("Database connection failed. Please run init_database.py first.")
        sys.exit(1)
    
    try:
        # Start with sample data (last 50 years)
        logger.info("Collecting sample data (last 50 years)...")
        sample_result = await collect_sample_data(sample_years=50)
        
        if sample_result.get('success', False):
            logger.info(f"Sample data collection completed successfully!")
            logger.info(f"Events collected: {sample_result.get('total_events_collected', 0)}")
            logger.info(f"Events saved: {sample_result.get('total_events_saved', 0)}")
            
            # If sample was successful, collect more historical data
            choice = input("\nSample data collected successfully. Do you want to collect full historical data (1-2025)? This may take 2-4 hours. [y/N]: ")
            
            if choice.lower() in ['y', 'yes']:
                logger.info("Starting full historical data collection...")
                full_result = await collect_historical_data(start_year=1, end_year=2025)
                
                if full_result.get('success', False):
                    logger.info("Full historical data collection completed successfully!")
                    logger.info(f"Total events collected: {full_result.get('total_events_collected', 0)}")
                    logger.info(f"Total events saved: {full_result.get('total_events_saved', 0)}")
                else:
                    logger.warning("Full data collection completed with errors.")
                    logger.warning(f"Error: {full_result.get('error_message', 'Unknown error')}")
            else:
                logger.info("Skipping full historical collection. You can run it later using:")
                logger.info("python scripts/collect_full_data.py")
        
        else:
            logger.error("Sample data collection failed.")
            logger.error(f"Error: {sample_result.get('error_message', 'Unknown error')}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("Data collection interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Data collection failed: {str(e)}")
        sys.exit(1)
    
    logger.info("Initial data collection completed!")

if __name__ == "__main__":
    asyncio.run(main())
