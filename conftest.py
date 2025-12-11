"""Pytest configuration for async tests"""
import pytest
import pytest_asyncio
import asyncio
import asyncpg
from contextlib import asynccontextmanager
import sys
import os

# Add aggregator to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'aggregator'))

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)

@pytest.fixture(scope='session')
def event_loop_policy():
    """Use the default event loop policy"""
    return asyncio.DefaultEventLoopPolicy()

# Mock database for testing
class MockDatabase:
    def __init__(self):
        self.events = {}  # (topic, event_id) -> event
        self.processed = {}  # (topic, event_id) -> True
        self.stats = {
            'received': 0,
            'processed': 0,
            'duplicates': 0
        }
    
    async def add_event(self, topic: str, event_id: str, event: dict) -> bool:
        """Add event, return True if new, False if duplicate"""
        key = (topic, event_id)
        if key in self.processed:
            self.stats['duplicates'] += 1
            return False
        
        self.events[key] = event
        self.processed[key] = True
        self.stats['processed'] += 1
        return True
    
    def reset(self):
        """Reset all data"""
        self.events.clear()
        self.processed.clear()
        self.stats = {'received': 0, 'processed': 0, 'duplicates': 0}

# Global mock database instance
_mock_db = MockDatabase()

@pytest.fixture
def mock_db():
    """Provide mock database for tests"""
    _mock_db.reset()
    return _mock_db
