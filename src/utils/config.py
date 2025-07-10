"""
Configuration module for Nine Cycle project.
Handles environment variables and application settings.
"""

import os
from pathlib import Path
from typing import Optional, List

try:
    # Pydantic v2
    from pydantic import BaseSettings, Field, field_validator
    PYDANTIC_V2 = True
except ImportError:
    try:
        # Pydantic v1
        from pydantic import BaseSettings, Field, validator
        PYDANTIC_V2 = False
    except ImportError:
        # Fallback - create minimal settings class
        BaseSettings = object
        Field = lambda default=None, **kwargs: default
        PYDANTIC_V2 = False
        
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings(BaseSettings if BaseSettings != object else object):
    """Application settings configuration."""
    
    # Project paths
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent
    DATA_RAW_PATH: Path = Path("data/raw")
    DATA_PROCESSED_PATH: Path = Path("data/processed") 
    DATA_CYCLES_PATH: Path = Path("data/cycles")
    DATA_EXPORTS_PATH: Path = Path("data/exports")
    LOGS_PATH: Path = Path("logs")
    MODELS_PATH: Path = Path("models")
    
    # Database configuration
    DATABASE_URL: str = "postgresql://nine_cycle_user:password@localhost:5432/nine_cycle_db"
    POSTGRES_USER: str = "nine_cycle_user"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_DB: str = "nine_cycle_db"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    
    # API Keys
    WORLD_BANK_API_KEY: Optional[str] = None
    ALPHA_VANTAGE_API_KEY: Optional[str] = None
    NEWS_API_KEY: Optional[str] = None
    FRED_API_KEY: Optional[str] = None
    
    # Application settings
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"
    SECRET_KEY: str = "dev-secret-key-change-in-production"
    
    # Data collection settings
    MAX_REQUESTS_PER_MINUTE: int = 60
    DATA_COLLECTION_BATCH_SIZE: int = 100
    RETRY_ATTEMPTS: int = 3
    TIMEOUT_SECONDS: int = 30
    
    # Rate limiting settings
    WIKIPEDIA_RATE_LIMIT: float = 1.0  # seconds between requests
    WORLDBANK_RATE_LIMIT: float = 0.5
    NEWS_API_RATE_LIMIT: float = 10.0
    
    # Dashboard settings
    DASH_HOST: str = "0.0.0.0"
    DASH_PORT: int = 8050
    DASH_DEBUG: bool = True
    
    # API settings
    FASTAPI_HOST: str = "0.0.0.0"
    FASTAPI_PORT: int = 8000
    
    # ML Model settings
    MODEL_VERSION: str = "1.0.0"
    PREDICTION_CONFIDENCE_THRESHOLD: float = 0.7
    VALIDATION_SPLIT: float = 0.2
    TEST_SPLIT: float = 0.2
    
    # Monitoring settings
    ENABLE_MONITORING: bool = True
    ALERT_EMAIL: Optional[str] = None
    SLACK_WEBHOOK_URL: Optional[str] = None
    
    def __init__(self, **kwargs):
        """Initialize settings with environment variable support."""
        # Load from environment variables
        for key in dir(self):
            if not key.startswith('_') and key.isupper():
                env_value = os.getenv(key)
                if env_value is not None:
                    # Try to convert to appropriate type
                    current_value = getattr(self, key)
                    if isinstance(current_value, bool):
                        setattr(self, key, env_value.lower() in ('true', '1', 'yes', 'on'))
                    elif isinstance(current_value, int):
                        try:
                            setattr(self, key, int(env_value))
                        except ValueError:
                            pass
                    elif isinstance(current_value, float):
                        try:
                            setattr(self, key, float(env_value))
                        except ValueError:
                            pass
                    else:
                        setattr(self, key, env_value)
        
        # Convert relative paths to absolute paths
        for attr_name in ['DATA_RAW_PATH', 'DATA_PROCESSED_PATH', 'DATA_CYCLES_PATH', 
                         'DATA_EXPORTS_PATH', 'LOGS_PATH', 'MODELS_PATH']:
            path_val = getattr(self, attr_name)
            if isinstance(path_val, (str, Path)) and not Path(path_val).is_absolute():
                setattr(self, attr_name, self.PROJECT_ROOT / path_val)
            else:
                setattr(self, attr_name, Path(path_val))
    
    # Remove validator methods for now to avoid pydantic compatibility issues
    
    def create_directories(self):
        """Create necessary directories if they don't exist."""
        directories = [
            self.DATA_RAW_PATH,
            self.DATA_PROCESSED_PATH,
            self.DATA_CYCLES_PATH,
            self.DATA_EXPORTS_PATH,
            self.LOGS_PATH,
            self.MODELS_PATH,
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    # Config class for compatibility
    if hasattr(BaseSettings, '__config__'):
        class Config:
            env_file = ".env"
            case_sensitive = True

# Global settings instance
settings = Settings()

# Create directories on import
settings.create_directories()

# Historical event categories for data collection
HISTORICAL_CATEGORIES = {
    'economic': [
        'recession', 'depression', 'stock market crash', 'inflation',
        'currency crisis', 'debt crisis', 'economic boom', 'financial crisis',
        'bankruptcy', 'market volatility', 'commodity price shock'
    ],
    'political': [
        'war', 'revolution', 'coup', 'election', 'treaty', 'independence',
        'civil war', 'regime change', 'political crisis', 'diplomatic crisis',
        'sanctions', 'alliance formation', 'peace agreement'
    ],
    'technological': [
        'invention', 'discovery', 'breakthrough', 'innovation', 'patent',
        'industrial revolution', 'digital revolution', 'space exploration',
        'medical breakthrough', 'scientific discovery', 'technological disruption'
    ],
    'social': [
        'social movement', 'civil rights', 'cultural shift', 'demographic change',
        'migration', 'urbanization', 'education reform', 'religious movement',
        'gender equality', 'labor movement', 'protests', 'social reform'
    ],
    'environmental': [
        'natural disaster', 'climate event', 'pandemic', 'epidemic',
        'environmental crisis', 'resource depletion', 'conservation',
        'pollution', 'extinction', 'geological event', 'weather extreme'
    ]
}

# Event severity levels for scoring
EVENT_SEVERITY_LEVELS = {
    'global': 5,      # Affects multiple continents
    'continental': 4,  # Affects entire continent
    'national': 3,     # Affects entire country
    'regional': 2,     # Affects region/state
    'local': 1         # Local significance only
}

# Digital root mapping for cycle analysis
DIGITAL_ROOT_CYCLES = {
    1: "New beginnings, leadership, innovation",
    2: "Cooperation, diplomacy, partnerships", 
    3: "Creativity, communication, expansion",
    4: "Structure, stability, foundation building",
    5: "Change, freedom, transformation",
    6: "Responsibility, service, community",
    7: "Spirituality, analysis, introspection",
    8: "Material success, power, achievement",
    9: "Completion, humanitarian, universal"
}

# Data source configurations
DATA_SOURCES = {
    'wikipedia': {
        'base_url': 'https://en.wikipedia.org',
        'api_url': 'https://en.wikipedia.org/w/api.php',
        'rate_limit': settings.WIKIPEDIA_RATE_LIMIT,
        'timeout': settings.TIMEOUT_SECONDS
    },
    'worldbank': {
        'base_url': 'https://api.worldbank.org/v2',
        'indicators': ['NY.GDP.MKTP.CD', 'FP.CPI.TOTL.ZG', 'SL.UEM.TOTL.ZS'],
        'rate_limit': settings.WORLDBANK_RATE_LIMIT,
        'timeout': settings.TIMEOUT_SECONDS
    },
    'newsapi': {
        'base_url': 'https://newsapi.org/v2',
        'rate_limit': settings.NEWS_API_RATE_LIMIT,
        'timeout': settings.TIMEOUT_SECONDS
    },
    'fred': {
        'base_url': 'https://api.stlouisfed.org/fred',
        'rate_limit': 1.0,
        'timeout': settings.TIMEOUT_SECONDS
    }
}
