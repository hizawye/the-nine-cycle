"""
Test the data collection module functionality.
"""

import sys
import asyncio
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from src.collectors.base_collector import CollectedEvent
from src.utils.config import settings
from src.utils.logging_config import setup_logging

def test_collected_event():
    """Test CollectedEvent class functionality."""
    print("Testing CollectedEvent class...")
    
    # Create test event
    event = CollectedEvent(
        year=2008,
        title="Global Financial Crisis",
        description="Major financial crisis affecting global markets",
        category="economic",
        source="test",
        severity=5
    )
    
    # Test digital root calculation
    assert event.digital_root == 1, f"Expected digital root 1, got {event.digital_root}"
    print(f"✓ Digital root correctly calculated: {event.digital_root}")
    
    # Test data conversion
    event_dict = event.to_dict()
    assert 'year' in event_dict
    assert 'digital_root' in event_dict
    print("✓ Event conversion to dict works")
    
    # Test hash generation
    event_hash = event.get_hash()
    assert len(event_hash) == 32, f"Expected 32-char hash, got {len(event_hash)}"
    print(f"✓ Event hash generated: {event_hash[:8]}...")

def test_configuration():
    """Test configuration loading."""
    print("\nTesting configuration...")
    
    # Test settings access
    assert hasattr(settings, 'DATABASE_URL')
    assert hasattr(settings, 'PROJECT_ROOT')
    print("✓ Settings loaded successfully")
    
    # Test directory creation
    settings.create_directories()
    print("✓ Directories created/verified")

def test_logging():
    """Test logging configuration."""
    print("\nTesting logging setup...")
    
    setup_logging()
    logger = logging.getLogger('test')
    
    logger.info("Test info message")
    logger.warning("Test warning message") 
    logger.error("Test error message")
    
    print("✓ Logging configured and working")

async def test_rate_limiter():
    """Test rate limiting functionality."""
    from src.collectors.base_collector import RateLimiter
    import time
    
    print("\nTesting rate limiter...")
    
    limiter = RateLimiter(0.1)  # 100ms between requests
    
    start_time = time.time()
    await limiter.wait()
    await limiter.wait()
    await limiter.wait()
    end_time = time.time()
    
    elapsed = end_time - start_time
    assert elapsed >= 0.2, f"Expected at least 200ms, got {elapsed*1000:.1f}ms"
    print(f"✓ Rate limiter working: {elapsed*1000:.1f}ms elapsed")

def test_digital_root_calculation():
    """Test digital root calculation for various years."""
    print("\nTesting digital root calculations...")
    
    test_cases = [
        (1, 1),
        (9, 9), 
        (10, 1),
        (19, 1),
        (2008, 1),
        (2023, 7),
        (1999, 1),
        (1929, 4)
    ]
    
    for year, expected_root in test_cases:
        event = CollectedEvent(
            year=year,
            title="Test",
            description="Test",
            category="test",
            source="test"
        )
        assert event.digital_root == expected_root, f"Year {year}: expected {expected_root}, got {event.digital_root}"
    
    print("✓ All digital root calculations correct")

async def main():
    """Run all tests."""
    print("Nine Cycle Data Collection Module Tests")
    print("=" * 50)
    
    try:
        # Run tests
        test_collected_event()
        test_configuration() 
        test_logging()
        await test_rate_limiter()
        test_digital_root_calculation()
        
        print("\n" + "=" * 50)
        print("✅ All tests passed! Data collection module is working correctly.")
        print("\nNext steps:")
        print("1. Set up your .env file with database credentials")
        print("2. Run: python scripts/init_database.py")
        print("3. Run: python scripts/collect_initial_data.py")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
