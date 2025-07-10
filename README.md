# The Nine Cycle Project

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## 🎯 Overview

The Nine Cycle project is a comprehensive data science platform that investigates whether major historical events follow predictable 9-year cycles based on digital root analysis. By analyzing patterns in economic, political, technological, social, and environmental events from 1 AD to 2025 AD, this project aims to uncover potential cyclical patterns in human history.

### Key Features

- **Multi-source Data Collection**: Automated collection from Wikipedia, World Bank, News API, and economic data sources
- **Digital Root Analysis**: Mathematical analysis of years using digital root calculations
- **Pattern Detection**: Advanced algorithms to identify cyclical patterns in historical events
- **Real-time Processing**: Asynchronous data collection and processing with rate limiting
- **Data Validation**: Comprehensive validation and quality scoring of collected data
- **Professional Architecture**: Clean, modular design following software engineering best practices

## 🚀 Quick Start

### Prerequisites

- **Python**: 3.10 or higher
- **Database**: PostgreSQL 15+ (or Docker)
- **Memory**: 16GB RAM recommended for full dataset processing
- **Storage**: 10GB+ free space for historical data
- **Network**: Stable internet connection for API data collection

### Installation

1. **Clone the repository:**
```bash
git clone https://github.com/hizawye/theninecycle.git
cd theninecycle
```

2. **Create and activate virtual environment:**
```bash
python -m venv nine_cycle_env
source nine_cycle_env/bin/activate  # Linux/macOS
# nine_cycle_env\Scripts\activate  # Windows PowerShell
```

3. **Install dependencies:**
```bash
# Core dependencies only
pip install -r requirements-core.txt

# Or full dependencies (includes ML packages)
pip install -r requirements.txt
```

4. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your database credentials and API keys
```

5. **Start services with Docker:**
```bash
docker-compose up -d postgres redis
```

6. **Initialize database:**
```bash
python scripts/init_database.py
```

7. **Verify installation:**
```bash
python scripts/test_connection.py
python scripts/test_basic.py
```

### First Data Collection

```bash
# Collect sample data (recommended for testing)
python scripts/collect_initial_data.py

# Check collection status
python -c "
from src.utils.database import get_database_manager
db = get_database_manager()
count = db.get_event_count()
print(f'Total events in database: {count}')
"
```

## 📊 Data Collection

The project collects historical events from multiple sources:

- **Wikipedia**: Historical events from year pages and categories
- **World Bank**: Economic indicators and crisis detection
- **News API**: Recent events and developments
- **Historical Database**: Known major events with verification

### Running Data Collection

```bash
# Collect sample data (last 50 years)
python -c "
import asyncio
from src.data_collection import collect_sample_data
result = asyncio.run(collect_sample_data(50))
print(f'Collected {result[\"total_events_collected\"]} events')
"

# Collect full historical data (1-2025 AD)
python -c "
import asyncio  
from src.data_collection import collect_historical_data
result = asyncio.run(collect_historical_data(1, 2025))
print(f'Collected {result[\"total_events_collected\"]} events')
"
```

## 🔧 Project Structure

```
nine-cycle/
├── src/
│   ├── collectors/          # Data collection modules
│   │   ├── base_collector.py
│   │   ├── wikipedia_collector.py
│   │   ├── economic_collector.py
│   │   └── news_collector.py
│   ├── utils/              # Utility modules
│   │   ├── config.py
│   │   ├── database.py
│   │   ├── logging_config.py
│   │   └── data_validation.py
│   └── data_collection.py  # Main orchestrator
├── scripts/                # Utility scripts
│   ├── init_database.py
│   ├── collect_initial_data.py
│   └── test_connection.py
├── data/                   # Data storage
├── logs/                   # Application logs
└── requirements.txt        # Dependencies
```

## 📈 Core Concepts

### Digital Root Analysis

The digital root of a year is calculated by summing its digits until a single digit remains:
- 2008 → 2+0+0+8 = 10 → 1+0 = 1
- Years with the same digital root are hypothesized to share similar event patterns

### 9-Year Cycles

The project investigates whether events cluster around specific digital roots in repeating 9-year cycles:
- Digital root 1: New beginnings, leadership, innovation
- Digital root 2: Cooperation, diplomacy, partnerships
- Digital root 3: Creativity, communication, expansion
- And so on...

## 🛠️ Development

### Database Schema

- **historical_events**: Main event storage with digital roots
- **cycle_patterns**: Detected patterns and their significance
- **data_collection_logs**: Collection activity tracking

### Adding New Data Sources

1. Create collector class inheriting from `BaseCollector`
2. Implement required methods: `collect_events()` and `categorize_event()`
3. Add to orchestrator in `data_collection.py`

### Running Tests

```bash
pytest tests/ -v
```

### Code Quality

```bash
black src/              # Format code
flake8 src/            # Lint code  
mypy src/              # Type checking
```

## 📝 Configuration

Key configuration options in `.env`:

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/nine_cycle_db

# API Keys
WORLD_BANK_API_KEY=your_key_here
ALPHA_VANTAGE_API_KEY=your_key_here  
NEWS_API_KEY=your_key_here

# Collection Settings
MAX_REQUESTS_PER_MINUTE=60
DATA_COLLECTION_BATCH_SIZE=100
```

## 🧪 Example Usage

```python
from src.data_collection import DataCollectionOrchestrator
from src.utils.database import get_database_manager
from src.utils.data_validation import DataValidator

# Initialize components
orchestrator = DataCollectionOrchestrator()
db_manager = get_database_manager() 
validator = DataValidator()

# Collect data
result = await orchestrator.collect_all_data(1900, 2023)

# Get statistics
stats = orchestrator.get_collection_statistics()
print(f"Total events: {stats['overall_stats']['total_events']}")

# Validate data quality
validation = validator.generate_validation_report()
print(f"Data quality score: {validation['summary']['overall_score']}")
```

## 📊 Data Quality Metrics

The project maintains high data quality through:

- **Validation**: Automatic validation of all collected events
- **Deduplication**: Hash-based duplicate detection
- **Verification**: Manual verification of significant events  
- **Quality Scoring**: Comprehensive quality metrics

## 🔄 Next Steps

After successful data collection:

1. **Analysis Phase**: Implement cycle detection algorithms
2. **Statistical Validation**: Test pattern significance  
3. **Visualization**: Create interactive dashboards
4. **Prediction**: Build forecasting models

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open Pull Request

## 📞 Support

- **Issues**: Create GitHub issue with detailed description
- **Documentation**: Check the `.instructions` file for comprehensive guidelines
- **Contact**: @hizawye

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Built with**: Python, PostgreSQL, FastAPI, AsyncIO
**Research Focus**: Historical pattern analysis and digital root mathematics
