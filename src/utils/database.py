"""
Database utilities and connection management for Nine Cycle project.
"""

import logging
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, DateTime, Float, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import pandas as pd

from .config import settings

# Configure logging
logger = logging.getLogger(__name__)

# SQLAlchemy setup
Base = declarative_base()

class HistoricalEvent(Base):
    """Historical event model for database storage."""
    
    __tablename__ = 'historical_events'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False, index=True)
    date = Column(DateTime, nullable=True)
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    category = Column(String(100), nullable=False, index=True)
    subcategory = Column(String(100), nullable=True)
    severity = Column(Integer, nullable=True)
    digital_root = Column(Integer, nullable=False, index=True)
    source = Column(String(100), nullable=False)
    source_url = Column(String(1000), nullable=True)
    location = Column(String(200), nullable=True)
    participants = Column(Text, nullable=True)
    tags = Column(Text, nullable=True)  # JSON string of tags
    impact_score = Column(Float, nullable=True)
    verified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class CyclePattern(Base):
    """Detected cycle pattern model."""
    
    __tablename__ = 'cycle_patterns'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cycle_type = Column(String(50), nullable=False)  # e.g., "9-year"
    digital_root = Column(Integer, nullable=False)
    category = Column(String(100), nullable=False)
    pattern_strength = Column(Float, nullable=False)
    statistical_significance = Column(Float, nullable=True)
    event_count = Column(Integer, nullable=False)
    start_year = Column(Integer, nullable=False)
    end_year = Column(Integer, nullable=False)
    description = Column(Text, nullable=True)
    validation_status = Column(String(50), default='pending')
    created_at = Column(DateTime, default=datetime.utcnow)

class DataCollectionLog(Base):
    """Log of data collection activities."""
    
    __tablename__ = 'data_collection_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(100), nullable=False)
    collection_type = Column(String(100), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    status = Column(String(50), nullable=False)  # success, failed, in_progress
    records_collected = Column(Integer, default=0)
    errors_encountered = Column(Integer, default=0)
    error_details = Column(Text, nullable=True)
    collection_metadata = Column(Text, nullable=True)  # JSON string

class DatabaseManager:
    """Database connection and operations manager."""
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize database manager."""
        self.database_url = database_url or settings.DATABASE_URL
        self.engine = None
        self.SessionLocal = None
        self.connected = False
        
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.engine = create_engine(
                self.database_url,
                echo=settings.DEBUG,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.connected = True
            logger.info("Database connection established successfully")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            self.connected = False
            return False
    
    def create_tables(self):
        """Create all database tables."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def drop_tables(self):
        """Drop all database tables."""
        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.info("Database tables dropped successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to drop tables: {e}")
            raise
    
    @contextmanager
    def get_session(self):
        """Get database session with automatic cleanup."""
        if not self.connected:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def insert_historical_event(self, event_data: Dict[str, Any]) -> int:
        """Insert a historical event into the database."""
        with self.get_session() as session:
            event = HistoricalEvent(**event_data)
            session.add(event)
            session.flush()
            return event.id
    
    def bulk_insert_events(self, events_data: List[Dict[str, Any]]) -> int:
        """Bulk insert historical events."""
        with self.get_session() as session:
            events = [HistoricalEvent(**event_data) for event_data in events_data]
            session.bulk_save_objects(events)
            return len(events)
    
    def get_events_by_year_range(self, start_year: int, end_year: int) -> List[HistoricalEvent]:
        """Get events within a year range."""
        with self.get_session() as session:
            return session.query(HistoricalEvent).filter(
                HistoricalEvent.year >= start_year,
                HistoricalEvent.year <= end_year
            ).all()
    
    def get_events_by_digital_root(self, digital_root: int) -> List[HistoricalEvent]:
        """Get events by digital root value."""
        with self.get_session() as session:
            return session.query(HistoricalEvent).filter(
                HistoricalEvent.digital_root == digital_root
            ).all()
    
    def get_events_by_category(self, category: str) -> List[HistoricalEvent]:
        """Get events by category."""
        with self.get_session() as session:
            return session.query(HistoricalEvent).filter(
                HistoricalEvent.category == category
            ).all()
    
    def update_event_verification(self, event_id: int, verified: bool = True):
        """Update event verification status."""
        with self.get_session() as session:
            event = session.query(HistoricalEvent).filter(
                HistoricalEvent.id == event_id
            ).first()
            if event:
                event.verified = verified
                event.updated_at = datetime.utcnow()
    
    def log_collection_activity(self, log_data: Dict[str, Any]) -> int:
        """Log data collection activity."""
        with self.get_session() as session:
            log_entry = DataCollectionLog(**log_data)
            session.add(log_entry)
            session.flush()
            return log_entry.id
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get data collection statistics."""
        with self.get_session() as session:
            total_events = session.query(HistoricalEvent).count()
            verified_events = session.query(HistoricalEvent).filter(
                HistoricalEvent.verified == True
            ).count()
            
            category_counts = session.query(
                HistoricalEvent.category,
                session.query(HistoricalEvent).filter(
                    HistoricalEvent.category == HistoricalEvent.category
                ).count()
            ).group_by(HistoricalEvent.category).all()
            
            digital_root_counts = session.query(
                HistoricalEvent.digital_root,
                session.query(HistoricalEvent).filter(
                    HistoricalEvent.digital_root == HistoricalEvent.digital_root
                ).count()
            ).group_by(HistoricalEvent.digital_root).all()
            
            return {
                'total_events': total_events,
                'verified_events': verified_events,
                'verification_rate': verified_events / total_events if total_events > 0 else 0,
                'category_distribution': dict(category_counts),
                'digital_root_distribution': dict(digital_root_counts)
            }
    
    def export_events_to_dataframe(self, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Export events to pandas DataFrame."""
        with self.get_session() as session:
            query = session.query(HistoricalEvent)
            
            if filters:
                if 'start_year' in filters:
                    query = query.filter(HistoricalEvent.year >= filters['start_year'])
                if 'end_year' in filters:
                    query = query.filter(HistoricalEvent.year <= filters['end_year'])
                if 'category' in filters:
                    query = query.filter(HistoricalEvent.category == filters['category'])
                if 'verified_only' in filters and filters['verified_only']:
                    query = query.filter(HistoricalEvent.verified == True)
            
            events = query.all()
            
            # Convert to DataFrame
            data = []
            for event in events:
                data.append({
                    'id': event.id,
                    'year': event.year,
                    'date': event.date,
                    'title': event.title,
                    'description': event.description,
                    'category': event.category,
                    'subcategory': event.subcategory,
                    'severity': event.severity,
                    'digital_root': event.digital_root,
                    'source': event.source,
                    'location': event.location,
                    'impact_score': event.impact_score,
                    'verified': event.verified,
                    'created_at': event.created_at
                })
            
            return pd.DataFrame(data)

# Global database manager instance
db_manager = DatabaseManager()

def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    return db_manager

def init_database():
    """Initialize database connection and create tables."""
    success = db_manager.connect()
    if success:
        db_manager.create_tables()
        logger.info("Database initialized successfully")
    else:
        logger.error("Failed to initialize database")
        raise RuntimeError("Database initialization failed")

def test_database_connection() -> bool:
    """Test database connection."""
    try:
        db_manager.connect()
        return db_manager.connected
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False
