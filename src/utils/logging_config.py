"""
Logging configuration for Nine Cycle project.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
try:
    import structlog
    HAS_STRUCTLOG = True
except ImportError:
    HAS_STRUCTLOG = False
    structlog = None

try:
    from loguru import logger as loguru_logger
    HAS_LOGURU = True
except ImportError:
    HAS_LOGURU = False
    loguru_logger = None

from .config import settings

class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels."""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.RESET}"
        return super().format(record)

def setup_logging(
    log_level: str = None,
    log_file: Optional[str] = None,
    enable_structlog: bool = True
):
    """
    Setup comprehensive logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        enable_structlog: Whether to enable structured logging
    """
    log_level = log_level or settings.LOG_LEVEL
    
    # Ensure logs directory exists
    settings.LOGS_PATH.mkdir(parents=True, exist_ok=True)
    
    # Clear existing handlers
    logging.getLogger().handlers.clear()
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    
    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level))
    console_formatter = ColoredFormatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler for general logs
    general_log_file = settings.LOGS_PATH / 'nine_cycle.log'
    file_handler = logging.handlers.RotatingFileHandler(
        general_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Error-specific file handler
    error_log_file = settings.LOGS_PATH / 'errors.log'
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    root_logger.addHandler(error_handler)
    
    # Data collection specific log
    data_log_file = settings.LOGS_PATH / 'data_collection.log'
    data_handler = logging.handlers.RotatingFileHandler(
        data_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=10
    )
    data_handler.setLevel(logging.INFO)
    data_handler.setFormatter(file_formatter)
    
    # Add data handler to specific loggers
    data_loggers = [
        'nine_cycle.collectors',
        'nine_cycle.analyzers',
        'nine_cycle.data_collection'
    ]
    for logger_name in data_loggers:
        logger = logging.getLogger(logger_name)
        logger.addHandler(data_handler)
    
    # Setup structured logging with structlog
    if enable_structlog and HAS_STRUCTLOG:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    elif enable_structlog:
        logger.warning("Structlog not available, skipping structured logging setup")
    
    # Setup loguru for additional features
    if HAS_LOGURU:
        loguru_logger.remove()  # Remove default handler
        
        # Add loguru file handler for analysis logs
        analysis_log_file = settings.LOGS_PATH / 'analysis.log'
        loguru_logger.add(
            analysis_log_file,
            rotation="100 MB",
            retention="30 days",
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
            serialize=True
        )
    
    # Custom log file if specified
    if log_file:
        custom_handler = logging.FileHandler(log_file)
        custom_handler.setLevel(getattr(logging, log_level))
        custom_handler.setFormatter(file_formatter)
        root_logger.addHandler(custom_handler)
    
    logging.info(f"Logging configured - Level: {log_level}, Logs directory: {settings.LOGS_PATH}")

def get_logger(name: str) -> logging.Logger:
    """Get a named logger instance."""
    return logging.getLogger(name)

def get_structured_logger(name: str):
    """Get a structured logger instance."""
    if HAS_STRUCTLOG:
        return structlog.get_logger(name)
    else:
        return logging.getLogger(name)

def log_function_call(func):
    """Decorator to log function calls with parameters and execution time."""
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = datetime.now()
        
        # Log function entry
        logger.debug(f"Entering {func.__name__} with args={args}, kwargs={kwargs}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.debug(f"Completed {func.__name__} in {execution_time:.3f}s")
            return result
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Error in {func.__name__} after {execution_time:.3f}s: {str(e)}")
            raise
    
    return wrapper

def log_data_collection_event(
    source: str,
    event_type: str,
    status: str,
    records_count: int = 0,
    error_message: str = None,
    metadata: dict = None
):
    """Log data collection events with structured information."""
    logger = get_structured_logger('nine_cycle.data_collection')
    
    log_data = {
        'source': source,
        'event_type': event_type,
        'status': status,
        'records_count': records_count,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if error_message:
        log_data['error_message'] = error_message
    
    if metadata:
        log_data['metadata'] = metadata
    
    if status == 'success':
        logger.info("Data collection completed", **log_data)
    elif status == 'error':
        logger.error("Data collection failed", **log_data)
    else:
        logger.info("Data collection event", **log_data)

def log_analysis_event(
    analysis_type: str,
    status: str,
    input_records: int = 0,
    output_records: int = 0,
    patterns_found: int = 0,
    confidence_score: float = None,
    metadata: dict = None
):
    """Log analysis events with structured information."""
    logger = get_structured_logger('nine_cycle.analysis')
    
    log_data = {
        'analysis_type': analysis_type,
        'status': status,
        'input_records': input_records,
        'output_records': output_records,
        'patterns_found': patterns_found,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if confidence_score is not None:
        log_data['confidence_score'] = confidence_score
    
    if metadata:
        log_data['metadata'] = metadata
    
    logger.info("Analysis event", **log_data)

class DataCollectionLogger:
    """Specialized logger for data collection activities."""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = get_logger(f'nine_cycle.collectors.{source_name}')
        self.structured_logger = get_structured_logger(f'nine_cycle.collectors.{source_name}')
    
    def start_collection(self, collection_type: str, target_count: int = None):
        """Log start of data collection."""
        message = f"Starting {collection_type} collection from {self.source_name}"
        if target_count:
            message += f" (target: {target_count} records)"
        
        self.logger.info(message)
        log_data_collection_event(
            source=self.source_name,
            event_type=collection_type,
            status='started',
            metadata={'target_count': target_count}
        )
    
    def log_progress(self, collected: int, total: int = None, message: str = None):
        """Log collection progress."""
        if total:
            percentage = (collected / total) * 100
            progress_msg = f"Progress: {collected}/{total} ({percentage:.1f}%)"
        else:
            progress_msg = f"Collected: {collected} records"
        
        if message:
            progress_msg += f" - {message}"
        
        self.logger.info(progress_msg)
    
    def log_success(self, collection_type: str, records_collected: int, duration: float = None):
        """Log successful collection completion."""
        message = f"Completed {collection_type} collection: {records_collected} records"
        if duration:
            message += f" in {duration:.2f}s"
        
        self.logger.info(message)
        log_data_collection_event(
            source=self.source_name,
            event_type=collection_type,
            status='success',
            records_count=records_collected,
            metadata={'duration_seconds': duration}
        )
    
    def log_error(self, collection_type: str, error: Exception, records_collected: int = 0):
        """Log collection error."""
        error_message = f"Error in {collection_type} collection: {str(error)}"
        self.logger.error(error_message, exc_info=True)
        log_data_collection_event(
            source=self.source_name,
            event_type=collection_type,
            status='error',
            records_count=records_collected,
            error_message=str(error)
        )
    
    def log_warning(self, message: str, metadata: dict = None):
        """Log warning message."""
        self.logger.warning(message)
        if metadata:
            self.structured_logger.warning(message, **metadata)

# Initialize logging on module import
setup_logging()
