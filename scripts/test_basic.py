"""
Simple test script to verify basic functionality without external dependencies.
"""

import sys
import asyncio
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_basic_imports():
    """Test that basic modules can be imported."""
    print("Testing basic imports...")
    
    try:
        from src.utils.config import settings
        print("✓ Config module imported successfully")
    except Exception as e:
        print(f"✗ Config import failed: {e}")
        return False
    
    try:
        from src.utils.database import HistoricalEvent
        print("✓ Database models imported successfully")
    except Exception as e:
        print(f"✗ Database import failed: {e}")
        return False
    
    try:
        from src.collectors.base_collector import CollectedEvent
        print("✓ Base collector imported successfully")
    except Exception as e:
        print(f"✗ Base collector import failed: {e}")
        return False
    
    try:
        from src.data_collection import DataCollectionOrchestrator
        print("✓ Data collection orchestrator imported successfully")
    except Exception as e:
        print(f"✗ Data collection orchestrator import failed: {e}")
        return False
    
    return True

def test_digital_root():
    """Test digital root calculation."""
    print("\nTesting digital root calculation...")
    
    from src.collectors.base_collector import CollectedEvent
    
    test_cases = [
        (2008, 1),
        (2023, 7),
        (1999, 1),
        (1929, 3),  # 1+9+2+9=21, 2+1=3
        (1, 1),
        (9, 9),
        (10, 1),
    ]
    
    for year, expected in test_cases:
        event = CollectedEvent(
            year=year,
            title="Test Event",
            description="Test Description",
            category="test",
            source="test"
        )
        if event.digital_root != expected:
            print(f"✗ Digital root test failed for {year}: expected {expected}, got {event.digital_root}")
            return False
    
    print("✓ All digital root calculations correct")
    return True

def test_configuration():
    """Test configuration loading."""
    print("\nTesting configuration...")
    
    from src.utils.config import settings
    
    # Test that basic settings exist
    required_settings = ['DATABASE_URL', 'PROJECT_ROOT', 'DATA_RAW_PATH']
    
    for setting in required_settings:
        if not hasattr(settings, setting):
            print(f"✗ Missing required setting: {setting}")
            return False
    
    print("✓ Configuration loaded successfully")
    return True

def test_event_creation():
    """Test event creation and validation."""
    print("\nTesting event creation...")
    
    from src.collectors.base_collector import CollectedEvent
    
    # Create test event
    event = CollectedEvent(
        year=2020,
        title="COVID-19 Pandemic",
        description="Global pandemic affecting worldwide economies and societies",
        category="environmental",
        source="test",
        severity=5
    )
    
    # Test basic properties
    if event.year != 2020:
        print(f"✗ Year property failed: expected 2020, got {event.year}")
        return False
    
    if event.digital_root != 4:  # 2+0+2+0 = 4
        print(f"✗ Digital root failed: expected 4, got {event.digital_root}")
        return False
    
    # Test dict conversion
    try:
        event_dict = event.to_dict()
        if 'year' not in event_dict or 'digital_root' not in event_dict:
            print(f"✗ Event dict conversion failed - missing keys. Present keys: {list(event_dict.keys())}")
            return False
    except Exception as e:
        print(f"✗ Event dict conversion failed with exception: {e}")
        return False
    
    print("✓ Event creation and conversion working")
    return True

async def main():
    """Run all basic tests."""
    print("Nine Cycle Project - Basic Functionality Test")
    print("=" * 50)
    
    all_tests_passed = True
    
    # Run tests
    test_functions = [
        test_basic_imports,
        test_configuration,
        test_digital_root,
        test_event_creation
    ]
    
    for test_func in test_functions:
        try:
            if not test_func():
                all_tests_passed = False
        except Exception as e:
            print(f"✗ Test {test_func.__name__} failed with exception: {e}")
            all_tests_passed = False
    
    print("\n" + "=" * 50)
    if all_tests_passed:
        print("✅ All basic tests passed!")
        print("\nThe core modules are working correctly.")
        print("You can now proceed with:")
        print("1. Install optional dependencies (pip install aiohttp pandas loguru)")
        print("2. Set up your .env file with database credentials")  
        print("3. Run: python scripts/init_database.py")
    else:
        print("❌ Some tests failed!")
        print("Please check the error messages above and fix any issues.")

if __name__ == "__main__":
    asyncio.run(main())
