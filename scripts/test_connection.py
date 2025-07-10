"""
Test database connection script for Nine Cycle project.
"""

import sys
import logging
from pathlib import Path

# Add src to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.utils.database import test_database_connection, get_database_manager
from src.utils.config import settings
from src.utils.logging_config import setup_logging

def main():
    """Test database connection and display status."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Testing Nine Cycle database connection...")
    
    try:
        # Display configuration
        logger.info(f"Database URL: {settings.DATABASE_URL}")
        logger.info(f"Host: {settings.POSTGRES_HOST}")
        logger.info(f"Port: {settings.POSTGRES_PORT}")
        logger.info(f"Database: {settings.POSTGRES_DB}")
        logger.info(f"User: {settings.POSTGRES_USER}")
        
        # Test connection
        if test_database_connection():
            logger.info("✅ Database connection successful!")
            
            # Get additional connection info
            db_manager = get_database_manager()
            if db_manager.connected:
                try:
                    stats = db_manager.get_collection_stats()
                    logger.info(f"Total events in database: {stats.get('total_events', 0)}")
                    logger.info(f"Verified events: {stats.get('verified_events', 0)}")
                    
                    if stats.get('total_events', 0) > 0:
                        logger.info("Database contains data and is ready for analysis.")
                    else:
                        logger.info("Database is empty. Run collect_initial_data.py to populate it.")
                
                except Exception as e:
                    logger.warning(f"Could not fetch database statistics: {str(e)}")
            
        else:
            logger.error("❌ Database connection failed!")
            logger.error("Please check your database configuration in .env file.")
            logger.error("Make sure PostgreSQL is running and accessible.")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Connection test failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
