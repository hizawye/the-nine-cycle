"""
Data validation utilities for Nine Cycle project.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json
from pathlib import Path

from .config import settings, HISTORICAL_CATEGORIES, EVENT_SEVERITY_LEVELS
from .database import get_database_manager

logger = logging.getLogger(__name__)

class DataValidator:
    """Data validation and quality control for collected events."""
    
    def __init__(self):
        """Initialize data validator."""
        self.db_manager = get_database_manager()
        self.validation_rules = {
            'year_range': (1, 2025),
            'title_length': (3, 500),
            'description_length': (0, 5000),
            'severity_range': (1, 5),
            'digital_root_range': (1, 9),
            'required_fields': ['year', 'title', 'category', 'source'],
            'valid_categories': list(HISTORICAL_CATEGORIES.keys()),
            'valid_severity_levels': list(EVENT_SEVERITY_LEVELS.values())
        }
    
    def validate_event(self, event_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a single event record.
        
        Args:
            event_data: Event data dictionary
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required fields
        for field in self.validation_rules['required_fields']:
            if field not in event_data or event_data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Validate year
        year = event_data.get('year')
        if year is not None:
            if not isinstance(year, int):
                errors.append("Year must be an integer")
            elif not (self.validation_rules['year_range'][0] <= year <= self.validation_rules['year_range'][1]):
                errors.append(f"Year {year} outside valid range {self.validation_rules['year_range']}")
        
        # Validate title
        title = event_data.get('title', '')
        if title:
            title_len = len(str(title))
            min_len, max_len = self.validation_rules['title_length']
            if not (min_len <= title_len <= max_len):
                errors.append(f"Title length {title_len} outside valid range {min_len}-{max_len}")
        
        # Validate description
        description = event_data.get('description', '')
        if description:
            desc_len = len(str(description))
            max_len = self.validation_rules['description_length'][1]
            if desc_len > max_len:
                errors.append(f"Description length {desc_len} exceeds maximum {max_len}")
        
        # Validate category
        category = event_data.get('category')
        if category and category not in self.validation_rules['valid_categories']:
            errors.append(f"Invalid category: {category}")
        
        # Validate severity
        severity = event_data.get('severity')
        if severity is not None:
            if not isinstance(severity, int):
                errors.append("Severity must be an integer")
            elif severity not in self.validation_rules['valid_severity_levels']:
                errors.append(f"Invalid severity level: {severity}")
        
        # Validate digital root
        digital_root = event_data.get('digital_root')
        if digital_root is not None:
            min_dr, max_dr = self.validation_rules['digital_root_range']
            if not (min_dr <= digital_root <= max_dr):
                errors.append(f"Digital root {digital_root} outside valid range {min_dr}-{max_dr}")
        
        # Validate URL format if present
        source_url = event_data.get('source_url')
        if source_url and not self.is_valid_url(source_url):
            errors.append("Invalid URL format")
        
        # Validate date format if present
        date = event_data.get('date')
        if date and not self.is_valid_date(date):
            errors.append("Invalid date format")
        
        return len(errors) == 0, errors
    
    def is_valid_url(self, url: str) -> bool:
        """Check if URL format is valid."""
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return url_pattern.match(url) is not None
    
    def is_valid_date(self, date_val: Any) -> bool:
        """Check if date is valid."""
        if isinstance(date_val, datetime):
            return True
        
        if isinstance(date_val, str):
            # Try common date formats
            date_formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ'
            ]
            
            for fmt in date_formats:
                try:
                    datetime.strptime(date_val, fmt)
                    return True
                except ValueError:
                    continue
        
        return False
    
    def validate_batch(self, events_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of events.
        
        Args:
            events_data: List of event data dictionaries
            
        Returns:
            Validation summary
        """
        total_events = len(events_data)
        valid_events = 0
        invalid_events = 0
        all_errors = []
        
        for i, event_data in enumerate(events_data):
            is_valid, errors = self.validate_event(event_data)
            
            if is_valid:
                valid_events += 1
            else:
                invalid_events += 1
                all_errors.append({
                    'event_index': i,
                    'event_title': event_data.get('title', 'Unknown'),
                    'errors': errors
                })
        
        validation_rate = (valid_events / total_events) * 100 if total_events > 0 else 0
        
        return {
            'total_events': total_events,
            'valid_events': valid_events,
            'invalid_events': invalid_events,
            'validation_rate': validation_rate,
            'errors': all_errors,
            'status': 'pass' if validation_rate >= 95 else 'fail'
        }
    
    def check_data_completeness(self) -> Dict[str, Any]:
        """Check completeness of data in database."""
        if not self.db_manager.connected:
            return {'error': 'Database not connected'}
        
        stats = self.db_manager.get_collection_stats()
        
        # Calculate completeness metrics
        total_events = stats['total_events']
        verified_events = stats['verified_events']
        category_distribution = stats['category_distribution']
        digital_root_distribution = stats['digital_root_distribution']
        
        # Check year coverage
        with self.db_manager.get_session() as session:
            year_coverage = session.execute(
                "SELECT MIN(year) as min_year, MAX(year) as max_year, COUNT(DISTINCT year) as unique_years FROM historical_events"
            ).fetchone()
        
        # Expected digital root distribution (should be roughly equal)
        expected_dr_distribution = {i: total_events / 9 for i in range(1, 10)}
        dr_distribution_variance = sum(
            abs(digital_root_distribution.get(i, 0) - expected_dr_distribution[i]) 
            for i in range(1, 10)
        ) / total_events if total_events > 0 else 0
        
        return {
            'total_events': total_events,
            'verified_events': verified_events,
            'verification_rate': (verified_events / total_events * 100) if total_events > 0 else 0,
            'year_coverage': {
                'min_year': year_coverage[0] if year_coverage[0] else None,
                'max_year': year_coverage[1] if year_coverage[1] else None,
                'unique_years': year_coverage[2] if year_coverage[2] else 0,
                'expected_years': 2025,  # 1 AD to 2025 AD
                'coverage_percentage': (year_coverage[2] / 2025 * 100) if year_coverage[2] else 0
            },
            'category_distribution': category_distribution,
            'digital_root_distribution': digital_root_distribution,
            'digital_root_variance': dr_distribution_variance,
            'data_quality_score': self.calculate_data_quality_score(stats, year_coverage, dr_distribution_variance)
        }
    
    def calculate_data_quality_score(self, stats: Dict, year_coverage: tuple, dr_variance: float) -> float:
        """Calculate overall data quality score (0-100)."""
        score = 0
        
        # Verification rate (40% of score)
        verification_rate = (stats['verified_events'] / stats['total_events'] * 100) if stats['total_events'] > 0 else 0
        score += (verification_rate / 100) * 40
        
        # Year coverage (30% of score)
        year_coverage_pct = (year_coverage[2] / 2025 * 100) if year_coverage[2] else 0
        score += min(year_coverage_pct / 100, 1.0) * 30
        
        # Category balance (15% of score)
        category_balance = 1.0 - (len(set(stats['category_distribution'].values())) - 1) / len(stats['category_distribution']) if stats['category_distribution'] else 0
        score += category_balance * 15
        
        # Digital root distribution (15% of score)
        dr_balance = max(0, 1.0 - dr_variance)
        score += dr_balance * 15
        
        return round(score, 2)
    
    def detect_duplicates(self) -> List[Dict[str, Any]]:
        """Detect potential duplicate events in database."""
        if not self.db_manager.connected:
            return []
        
        # Find events with similar titles and same year
        with self.db_manager.get_session() as session:
            duplicates = session.execute("""
                SELECT e1.id, e1.title, e1.year, e1.source,
                       e2.id as duplicate_id, e2.title as duplicate_title, e2.source as duplicate_source
                FROM historical_events e1
                JOIN historical_events e2 ON e1.year = e2.year 
                    AND e1.id < e2.id
                    AND similarity(e1.title, e2.title) > 0.8
                ORDER BY e1.year, e1.title
            """).fetchall()
        
        return [
            {
                'original_id': dup[0],
                'original_title': dup[1],
                'year': dup[2],
                'original_source': dup[3],
                'duplicate_id': dup[4],
                'duplicate_title': dup[5],
                'duplicate_source': dup[6]
            }
            for dup in duplicates
        ]
    
    def validate_digital_roots(self) -> Dict[str, Any]:
        """Validate that digital roots are correctly calculated."""
        if not self.db_manager.connected:
            return {'error': 'Database not connected'}
        
        with self.db_manager.get_session() as session:
            events = session.execute(
                "SELECT id, year, digital_root FROM historical_events"
            ).fetchall()
        
        incorrect_roots = []
        
        for event_id, year, stored_root in events:
            calculated_root = self.calculate_digital_root(year)
            if calculated_root != stored_root:
                incorrect_roots.append({
                    'event_id': event_id,
                    'year': year,
                    'stored_root': stored_root,
                    'calculated_root': calculated_root
                })
        
        accuracy_rate = ((len(events) - len(incorrect_roots)) / len(events) * 100) if events else 100
        
        return {
            'total_events': len(events),
            'incorrect_digital_roots': len(incorrect_roots),
            'accuracy_rate': accuracy_rate,
            'incorrect_events': incorrect_roots[:10],  # Show first 10 errors
            'status': 'pass' if accuracy_rate >= 99 else 'fail'
        }
    
    def calculate_digital_root(self, year: int) -> int:
        """Calculate digital root of a year."""
        while year > 9:
            year = sum(int(digit) for digit in str(year))
        return year
    
    def generate_validation_report(self, output_file: Optional[str] = None) -> Dict[str, Any]:
        """Generate comprehensive validation report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'data_completeness': self.check_data_completeness(),
            'digital_root_validation': self.validate_digital_roots(),
            'duplicate_detection': self.detect_duplicates()
        }
        
        # Add summary
        completeness = report['data_completeness']
        dr_validation = report['digital_root_validation']
        duplicates = report['duplicate_detection']
        
        report['summary'] = {
            'overall_score': completeness.get('data_quality_score', 0),
            'total_events': completeness.get('total_events', 0),
            'verification_rate': completeness.get('verification_rate', 0),
            'digital_root_accuracy': dr_validation.get('accuracy_rate', 0),
            'duplicate_count': len(duplicates),
            'status': 'pass' if (
                completeness.get('data_quality_score', 0) >= 80 and
                dr_validation.get('accuracy_rate', 0) >= 99 and
                len(duplicates) < 50
            ) else 'fail'
        }
        
        # Save report if output file specified
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Validation report saved to {output_path}")
        
        return report
    
    def clean_invalid_events(self, dry_run: bool = True) -> Dict[str, Any]:
        """Clean invalid events from database."""
        if not self.db_manager.connected:
            return {'error': 'Database not connected'}
        
        # Get all events and validate them
        with self.db_manager.get_session() as session:
            events = session.execute(
                "SELECT id, year, title, description, category, source, severity, digital_root FROM historical_events"
            ).fetchall()
        
        invalid_event_ids = []
        
        for event in events:
            event_data = {
                'id': event[0],
                'year': event[1],
                'title': event[2],
                'description': event[3],
                'category': event[4],
                'source': event[5],
                'severity': event[6],
                'digital_root': event[7]
            }
            
            is_valid, errors = self.validate_event(event_data)
            if not is_valid:
                invalid_event_ids.append(event[0])
        
        result = {
            'total_events': len(events),
            'invalid_events': len(invalid_event_ids),
            'invalid_event_ids': invalid_event_ids,
            'dry_run': dry_run
        }
        
        if not dry_run and invalid_event_ids:
            # Actually delete invalid events
            with self.db_manager.get_session() as session:
                deleted_count = session.execute(
                    "DELETE FROM historical_events WHERE id = ANY(:ids)",
                    {'ids': invalid_event_ids}
                ).rowcount
                result['deleted_count'] = deleted_count
                logger.info(f"Deleted {deleted_count} invalid events from database")
        
        return result

def validate_data_file(file_path: str) -> Dict[str, Any]:
    """Validate data from a JSON file."""
    validator = DataValidator()
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return validator.validate_batch(data)
        else:
            is_valid, errors = validator.validate_event(data)
            return {
                'total_events': 1,
                'valid_events': 1 if is_valid else 0,
                'invalid_events': 0 if is_valid else 1,
                'validation_rate': 100 if is_valid else 0,
                'errors': [{'event_index': 0, 'errors': errors}] if errors else [],
                'status': 'pass' if is_valid else 'fail'
            }
    
    except FileNotFoundError:
        return {'error': f'File not found: {file_path}'}
    except json.JSONDecodeError as e:
        return {'error': f'Invalid JSON: {str(e)}'}
    except Exception as e:
        return {'error': f'Validation error: {str(e)}'}

def check_database_integrity() -> Dict[str, Any]:
    """Check database integrity and consistency."""
    validator = DataValidator()
    return validator.generate_validation_report()
