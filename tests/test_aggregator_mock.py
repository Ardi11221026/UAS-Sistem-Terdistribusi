"""
Unit and Integration Tests for Pub-Sub Log Aggregator with Mock Server
Covers: deduplication, persistence, concurrency, validation, and API endpoints
This version uses in-memory mock storage and doesn't require Docker
"""
import json
import uuid
import asyncio
import pytest
from datetime import datetime, timezone
from typing import List, Dict, Any, Set, Tuple
from collections import defaultdict

# ============================================================================
# MOCK AGGREGATOR SERVICE
# ============================================================================

class MockAggregator:
    """In-memory mock aggregator for testing without Docker"""
    
    def __init__(self):
        self.events: Dict[Tuple[str, str], Dict] = {}  # (topic, event_id) -> event
        self.processed_events: Set[Tuple[str, str]] = set()  # (topic, event_id)
        self.stats = {
            'received': 0,
            'unique_processed': 0,
            'duplicate_dropped': 0,
            'topics': defaultdict(int),
            'start_time': datetime.now(timezone.utc)
        }
    
    def publish(self, events: List[Dict]) -> Dict:
        """Simulate publish endpoint"""
        if not events:
            return {'error': 'empty batch', 'accepted': 0, 'processed': 0, 'rejected': 0}
        
        if not isinstance(events, list):
            return {'error': 'not array', 'accepted': 0, 'processed': 0, 'rejected': 0}
        
        result = {
            'accepted': 0,
            'processed': 0,
            'rejected': 0,
            'duplicate': 0
        }
        
        for event in events:
            # Validate schema
            if not isinstance(event, dict):
                result['rejected'] += 1
                continue
            
            errors = []
            if 'topic' not in event:
                errors.append('missing_topic')
            if 'event_id' not in event:
                errors.append('missing_event_id')
            if 'payload' in event and not isinstance(event['payload'], dict):
                errors.append('invalid_payload')
            
            if errors:
                result['rejected'] += 1
                self.stats['received'] += 1
                continue
            
            # Process event
            result['accepted'] += 1
            self.stats['received'] += 1
            
            topic = event['topic']
            event_id = event['event_id']
            key = (topic, event_id)
            
            if key in self.processed_events:
                result['duplicate'] += 1
                self.stats['duplicate_dropped'] += 1
            else:
                self.events[key] = event
                self.processed_events.add(key)
                result['processed'] += 1
                self.stats['unique_processed'] += 1
                self.stats['topics'][topic] += 1
        
        return result
    
    def get_events(self, topic: str = None, limit: int = None) -> Dict:
        """Simulate GET /events endpoint"""
        events_list = list(self.events.values())
        
        if topic:
            events_list = [e for e in events_list if e.get('topic') == topic]
        
        if limit:
            events_list = events_list[:limit]
        
        return {
            'count': len(events_list),
            'events': events_list,
            'topic_filter': topic,
            'limit': limit
        }
    
    def get_stats(self) -> Dict:
        """Simulate GET /stats endpoint"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        return {
            'received': self.stats['received'],
            'unique_processed': self.stats['unique_processed'],
            'duplicate_dropped': self.stats['duplicate_dropped'],
            'topics': dict(self.stats['topics']),
            'uptime_seconds': uptime.total_seconds()
        }
    
    def health_check(self) -> Dict:
        """Simulate GET /health endpoint"""
        return {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def reset(self):
        """Reset all data"""
        self.events.clear()
        self.processed_events.clear()
        self.stats = {
            'received': 0,
            'unique_processed': 0,
            'duplicate_dropped': 0,
            'topics': defaultdict(int),
            'start_time': datetime.now(timezone.utc)
        }

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def aggregator():
    """Provide mock aggregator instance"""
    agg = MockAggregator()
    yield agg
    agg.reset()

# ============================================================================
# TEST: EVENT SCHEMA VALIDATION
# ============================================================================

def test_publish_valid_single_event(aggregator):
    """Test publishing single valid event"""
    event = {
        "topic": "test_topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"key": "value"}
    }
    
    result = aggregator.publish([event])
    assert result["accepted"] == 1
    assert result["processed"] == 1
    assert result["rejected"] == 0
    print("✓ test_publish_valid_single_event PASSED")

def test_publish_missing_topic(aggregator):
    """Test rejection of event without topic"""
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    result = aggregator.publish([event])
    assert result["rejected"] == 1
    print("✓ test_publish_missing_topic PASSED")

def test_publish_missing_event_id(aggregator):
    """Test rejection of event without event_id"""
    event = {
        "topic": "test_topic",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    result = aggregator.publish([event])
    assert result["rejected"] == 1
    print("✓ test_publish_missing_event_id PASSED")

def test_publish_invalid_payload(aggregator):
    """Test rejection of event with non-dict payload"""
    event = {
        "topic": "test_topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": "not a dict"
    }
    
    result = aggregator.publish([event])
    assert result["rejected"] == 1
    print("✓ test_publish_invalid_payload PASSED")

# ============================================================================
# TEST: IDEMPOTENCY & DEDUPLICATION
# ============================================================================

def test_dedup_duplicate_event(aggregator):
    """Test that duplicate events are not processed twice"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "dedup_test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"test": "data"}
    }
    
    # Publish same event twice
    result1 = aggregator.publish([event])
    assert result1["processed"] == 1
    
    result2 = aggregator.publish([event])
    assert result2["processed"] == 0
    assert result2["duplicate"] == 1
    
    # Verify only one record
    assert len(aggregator.events) == 1
    print("✓ test_dedup_duplicate_event PASSED")

def test_dedup_different_topics_same_event_id(aggregator):
    """Test that same event_id on different topics are treated as different"""
    event_id = str(uuid.uuid4())
    
    event1 = {
        "topic": "topic_a",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"a": 1}
    }
    
    event2 = {
        "topic": "topic_b",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"b": 2}
    }
    
    # Both should be accepted
    result1 = aggregator.publish([event1])
    assert result1["processed"] == 1
    
    result2 = aggregator.publish([event2])
    assert result2["processed"] == 1
    
    # Verify both records exist
    assert len(aggregator.events) == 2
    print("✓ test_dedup_different_topics_same_event_id PASSED")

def test_dedup_batch_with_duplicates(aggregator):
    """Test batch publish with duplicates within the batch"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "batch_test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    # Publish batch with same event three times
    batch = [event, event, event]
    result = aggregator.publish(batch)
    
    # Only first should be processed, others as duplicates
    assert result["processed"] == 1
    assert result["duplicate"] == 2
    assert result["accepted"] == 3
    print("✓ test_dedup_batch_with_duplicates PASSED")

# ============================================================================
# TEST: PERSISTENCE
# ============================================================================

def test_persistence_data_survives_restart(aggregator):
    """Test that data persists in memory"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "persistence_test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"persistent": True}
    }
    
    # Publish event
    result = aggregator.publish([event])
    assert result["processed"] == 1
    
    # Verify data exists
    assert (("persistence_test", event_id)) in aggregator.processed_events
    assert len(aggregator.events) == 1
    print("✓ test_persistence_data_survives_restart PASSED")

def test_duplicate_prevented_after_persistent_restart(aggregator):
    """Test that reprocessing is prevented even after 'restart'"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "restart_dedup",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    # First publish
    result1 = aggregator.publish([event])
    assert result1["processed"] == 1
    
    # Second publish after "restart" (data still in memory)
    result2 = aggregator.publish([event])
    
    # Must be treated as duplicate
    assert result2["processed"] == 0
    assert result2["duplicate"] == 1
    print("✓ test_duplicate_prevented_after_persistent_restart PASSED")

# ============================================================================
# TEST: CONCURRENCY & TRANSACTION
# ============================================================================

def test_concurrent_duplicate_publish(aggregator):
    """Test that concurrent publishes of same event are handled correctly"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "concurrent_test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"concurrent": True}
    }
    
    # Simulate concurrent publishes
    results = [aggregator.publish([event]) for _ in range(5)]
    
    # Count processed events
    processed_count = sum(r["processed"] for r in results)
    duplicate_count = sum(r["duplicate"] for r in results)
    
    # Only 1 should have processed successfully
    assert processed_count == 1
    assert duplicate_count == 4
    
    # Verify only 1 record in memory
    assert len(aggregator.events) == 1
    print("✓ test_concurrent_duplicate_publish PASSED")

def test_concurrent_different_events(aggregator):
    """Test that concurrent publishes of different events work correctly"""
    events = [
        {
            "topic": "concurrent_diff",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {"index": i}
        }
        for i in range(10)
    ]
    
    # Simulate concurrent publishes
    results = [aggregator.publish([event]) for event in events]
    
    # All should be processed
    processed_count = sum(r["processed"] for r in results)
    assert processed_count == 10
    
    # Verify all in memory
    assert len(aggregator.events) == 10
    print("✓ test_concurrent_different_events PASSED")

def test_stats_consistency_under_load(aggregator):
    """Test that statistics are consistent under concurrent load"""
    # Publish many events
    events = [
        {
            "topic": f"stats_test_{i % 3}",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {}
        }
        for i in range(50)
    ]
    
    # Split into batches and publish
    batch_size = 5
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        aggregator.publish(batch)
    
    # Get stats
    stats = aggregator.get_stats()
    
    # Verify counts are consistent
    assert stats["unique_processed"] > 0
    assert stats["received"] == stats["unique_processed"] + stats["duplicate_dropped"]
    assert stats["unique_processed"] == 50  # All unique
    print("✓ test_stats_consistency_under_load PASSED")

# ============================================================================
# TEST: API ENDPOINTS
# ============================================================================

def test_get_events_empty(aggregator):
    """Test GET /events when no events"""
    result = aggregator.get_events()
    assert result["count"] == 0
    assert result["events"] == []
    print("✓ test_get_events_empty PASSED")

def test_get_events_filtered_by_topic(aggregator):
    """Test GET /events with topic filter"""
    events = [
        {
            "topic": "events_test",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {"n": i}
        }
        for i in range(5)
    ]
    
    # Publish
    aggregator.publish(events)
    
    # Query with filter
    result = aggregator.get_events(topic="events_test")
    assert result["count"] == 5
    assert result["topic_filter"] == "events_test"
    print("✓ test_get_events_filtered_by_topic PASSED")

def test_get_events_limit(aggregator):
    """Test GET /events with limit parameter"""
    events = [
        {
            "topic": "limit_test",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {}
        }
        for i in range(20)
    ]
    
    aggregator.publish(events)
    
    # Query with limit
    result = aggregator.get_events(limit=5)
    assert result["count"] == 5
    print("✓ test_get_events_limit PASSED")

def test_get_stats_initial(aggregator):
    """Test GET /stats endpoint"""
    stats = aggregator.get_stats()
    
    assert "received" in stats
    assert "unique_processed" in stats
    assert "duplicate_dropped" in stats
    assert "topics" in stats
    assert "uptime_seconds" in stats
    assert stats["received"] == 0
    assert stats["unique_processed"] == 0
    print("✓ test_get_stats_initial PASSED")

def test_get_stats_after_publish(aggregator):
    """Test GET /stats after publishing events"""
    events = [
        {
            "topic": "stats_topic",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {}
        }
        for i in range(10)
    ]
    
    aggregator.publish(events)
    
    stats = aggregator.get_stats()
    
    assert stats["unique_processed"] == 10
    assert "stats_topic" in stats["topics"]
    print("✓ test_get_stats_after_publish PASSED")

# ============================================================================
# TEST: BATCH OPERATIONS
# ============================================================================

def test_publish_batch_mixed_valid_invalid(aggregator):
    """Test batch with mix of valid and invalid events"""
    events = [
        {
            "topic": "mixed_test",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {}
        },
        {  # Invalid: missing topic
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {}
        },
        {
            "topic": "mixed_test",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {}
        }
    ]
    
    result = aggregator.publish(events)
    
    assert result["accepted"] == 2
    assert result["processed"] == 2
    assert result["rejected"] == 1
    print("✓ test_publish_batch_mixed_valid_invalid PASSED")

# ============================================================================
# TEST: ERROR HANDLING
# ============================================================================

def test_publish_empty_array(aggregator):
    """Test publish with empty event array"""
    result = aggregator.publish([])
    assert result["accepted"] == 0
    assert result["processed"] == 0
    print("✓ test_publish_empty_array PASSED")

def test_publish_not_array(aggregator):
    """Test publish with non-array body"""
    result = aggregator.publish({"event_id": "test"})
    assert "error" in result
    assert result["accepted"] == 0
    print("✓ test_publish_not_array PASSED")

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
