"""
Database initialization script for Nine Cycle project.
"""

import sys
import logging
from pathlib import Path

# Add src to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.utils.config import settings
from src.utils.database import init_database, test_database_connection
from src.utils.logging_config import setup_logging

def main():
    """Initialize the database and create all tables."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Initializing Nine Cycle database...")
    
    try:
        # Test database connection first
        logger.info("Testing database connection...")
        if not test_database_connection():
            logger.error("Database connection test failed. Please check your configuration.")
            sys.exit(1)
        
        # Initialize database and create tables
        logger.info("Creating database tables...")
        init_database()
        
        logger.info("Database initialization completed successfully!")
        logger.info(f"Database URL: {settings.DATABASE_URL}")
        logger.info("You can now start collecting data.")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
