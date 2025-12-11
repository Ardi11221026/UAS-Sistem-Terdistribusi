"""
Unit and Integration Tests for Pub-Sub Log Aggregator
Covers: deduplication, persistence, concurrency, validation, and API endpoints
"""
import json
import uuid
import asyncio
import pytest
from datetime import datetime, timezone
from typing import List, Dict, Any

import httpx
import asyncpg

# ============================================================================
# FIXTURES
# ============================================================================

# Database configuration
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "user"
DB_PASSWORD = "password"
DB_NAME = "distributed_logs"
AGGREGATOR_URL = "http://localhost:8080"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@pytest.fixture
async def db_pool():
    """Create database connection pool for tests"""
    pool = await asyncpg.create_pool(DATABASE_URL)
    yield pool
    await pool.close()

@pytest.fixture
async def db_connection(db_pool):
    """Get single database connection"""
    async with db_pool.acquire() as conn:
        yield conn

@pytest.fixture
async def http_client():
    """Create HTTP client for API tests"""
    async with httpx.AsyncClient(base_url=AGGREGATOR_URL, timeout=30.0) as client:
        yield client

@pytest.fixture
async def cleanup_database(db_pool):
    """Cleanup database before and after tests"""
    async with db_pool.acquire() as conn:
        # Truncate tables
        await conn.execute("TRUNCATE TABLE event_log CASCADE;")
        await conn.execute("TRUNCATE TABLE processed_events CASCADE;")
        await conn.execute("TRUNCATE TABLE events CASCADE;")
        await conn.execute("UPDATE stats SET metric_value = 0;")
    
    yield
    
    async with db_pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE event_log CASCADE;")
        await conn.execute("TRUNCATE TABLE processed_events CASCADE;")
        await conn.execute("TRUNCATE TABLE events CASCADE;")
        await conn.execute("UPDATE stats SET metric_value = 0;")

# ============================================================================
# TEST: EVENT SCHEMA VALIDATION
# ============================================================================

@pytest.mark.asyncio
async def test_publish_valid_single_event(http_client, db_pool, cleanup_database):
    """Test publishing single valid event"""
    event = {
        "topic": "test_topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"key": "value"}
    }
    
    response = await http_client.post("/publish", json=[event])
    assert response.status_code == 200
    result = response.json()
    assert result["accepted"] == 1
    assert result["processed"] == 1
    assert result["rejected"] == 0

@pytest.mark.asyncio
async def test_publish_missing_topic(http_client, cleanup_database):
    """Test rejection of event without topic"""
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    response = await http_client.post("/publish", json=[event])
    assert response.status_code == 200
    result = response.json()
    assert result["rejected"] == 1

@pytest.mark.asyncio
async def test_publish_missing_event_id(http_client, cleanup_database):
    """Test rejection of event without event_id"""
    event = {
        "topic": "test_topic",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    response = await http_client.post("/publish", json=[event])
    assert response.status_code == 200
    result = response.json()
    assert result["rejected"] == 1

@pytest.mark.asyncio
async def test_publish_invalid_payload(http_client, cleanup_database):
    """Test rejection of event with non-dict payload"""
    event = {
        "topic": "test_topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": "not a dict"
    }
    
    response = await http_client.post("/publish", json=[event])
    assert response.status_code == 200
    result = response.json()
    assert result["rejected"] == 1

# ============================================================================
# TEST: IDEMPOTENCY & DEDUPLICATION
# ============================================================================

@pytest.mark.asyncio
async def test_dedup_duplicate_event(http_client, db_pool, cleanup_database):
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
    response1 = await http_client.post("/publish", json=[event])
    assert response1.json()["processed"] == 1
    
    response2 = await http_client.post("/publish", json=[event])
    assert response2.json()["processed"] == 0
    
    # Verify only one record in events table
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE topic = $1 AND event_id = $2",
            "dedup_test", event_id
        )
        assert count == 1

@pytest.mark.asyncio
async def test_dedup_different_topics_same_event_id(http_client, db_pool, cleanup_database):
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
    response1 = await http_client.post("/publish", json=[event1])
    assert response1.json()["processed"] == 1
    
    response2 = await http_client.post("/publish", json=[event2])
    assert response2.json()["processed"] == 1
    
    # Verify both records exist
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM events WHERE event_id = $1", event_id)
        assert count == 2

@pytest.mark.asyncio
async def test_dedup_batch_with_duplicates(http_client, db_pool, cleanup_database):
    """Test batch publish with duplicates within the batch"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "batch_test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    # Publish batch with same event twice
    batch = [event, event, event]
    response = await http_client.post("/publish", json=batch)
    result = response.json()
    
    # Only first should be processed, others as duplicates
    assert result["processed"] + result["duplicate"] == 1 or (
        result["processed"] == 1 and result["rejected"] == 2
    )

# ============================================================================
# TEST: PERSISTENCE
# ============================================================================

@pytest.mark.asyncio
async def test_persistence_data_survives_restart(http_client, db_pool, cleanup_database):
    """Test that data persists in database"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "persistence_test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"persistent": True}
    }
    
    # Publish event
    response = await http_client.post("/publish", json=[event])
    assert response.json()["processed"] == 1
    
    # Query database directly (simulating app restart)
    async with db_pool.acquire() as conn:
        stored = await conn.fetchval(
            "SELECT 1 FROM processed_events WHERE topic = $1 AND event_id = $2",
            "persistence_test", event_id
        )
        assert stored == 1
        
        # Verify it's marked as processed
        processed = await conn.fetchval(
            "SELECT COUNT(*) FROM processed_events WHERE topic = $1 AND event_id = $2",
            "persistence_test", event_id
        )
        assert processed == 1

@pytest.mark.asyncio
async def test_duplicate_prevented_after_persistent_restart(http_client, db_pool, cleanup_database):
    """Test that reprocessing is prevented even after data was persisted"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "restart_dedup",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {}
    }
    
    # First publish
    response1 = await http_client.post("/publish", json=[event])
    assert response1.json()["processed"] == 1
    
    # Simulate restart by checking database
    async with db_pool.acquire() as conn:
        await asyncio.sleep(0.1)  # Small delay
    
    # Second publish after "restart"
    response2 = await http_client.post("/publish", json=[event])
    result = response2.json()
    
    # Must be treated as duplicate
    assert result["processed"] == 0

# ============================================================================
# TEST: CONCURRENCY & TRANSACTION
# ============================================================================

@pytest.mark.asyncio
async def test_concurrent_duplicate_publish(http_client, db_pool, cleanup_database):
    """Test that concurrent publishes of same event are handled correctly"""
    event_id = str(uuid.uuid4())
    event = {
        "topic": "concurrent_test",
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"concurrent": True}
    }
    
    # Send same event concurrently (simulate race condition)
    tasks = [
        http_client.post("/publish", json=[event])
        for _ in range(5)
    ]
    responses = await asyncio.gather(*tasks)
    
    # Count processed events
    processed_count = sum(r.json()["processed"] for r in responses)
    
    # Only 1 should have processed successfully
    assert processed_count == 1
    
    # Verify only 1 record in database
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE topic = $1 AND event_id = $2",
            "concurrent_test", event_id
        )
        assert count == 1

@pytest.mark.asyncio
async def test_concurrent_different_events(http_client, db_pool, cleanup_database):
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
    
    # Send concurrently
    tasks = [
        http_client.post("/publish", json=[event])
        for event in events
    ]
    responses = await asyncio.gather(*tasks)
    
    # All should be processed
    processed_count = sum(r.json()["processed"] for r in responses)
    assert processed_count == 10
    
    # Verify all in database
    async with db_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE topic = $1",
            "concurrent_diff"
        )
        assert count == 10

@pytest.mark.asyncio
async def test_stats_consistency_under_load(http_client, db_pool, cleanup_database):
    """Test that statistics are consistent under concurrent load"""
    # Publish many events concurrently
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
    
    # Split into batches and publish concurrently
    batch_size = 5
    tasks = []
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        tasks.append(http_client.post("/publish", json=batch))
    
    responses = await asyncio.gather(*tasks)
    
    # Get stats
    stats_response = await http_client.get("/stats")
    stats = stats_response.json()
    
    # Verify counts are consistent
    assert stats["unique_processed"] > 0
    assert stats["received"] == stats["unique_processed"] + stats["duplicate_dropped"]

# ============================================================================
# TEST: API ENDPOINTS
# ============================================================================

@pytest.mark.asyncio
async def test_get_events_empty(http_client, cleanup_database):
    """Test GET /events when no events"""
    response = await http_client.get("/events")
    assert response.status_code == 200
    result = response.json()
    assert result["count"] == 0
    assert result["events"] == []

@pytest.mark.asyncio
async def test_get_events_filtered_by_topic(http_client, cleanup_database):
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
    await http_client.post("/publish", json=events)
    
    # Query with filter
    response = await http_client.get("/events?topic=events_test")
    assert response.status_code == 200
    result = response.json()
    assert result["count"] == 5
    assert result["topic_filter"] == "events_test"

@pytest.mark.asyncio
async def test_get_events_limit(http_client, cleanup_database):
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
    
    await http_client.post("/publish", json=events)
    
    # Query with limit
    response = await http_client.get("/events?limit=5")
    assert response.status_code == 200
    result = response.json()
    assert result["count"] == 5

@pytest.mark.asyncio
async def test_get_stats_initial(http_client, cleanup_database):
    """Test GET /stats endpoint"""
    response = await http_client.get("/stats")
    assert response.status_code == 200
    stats = response.json()
    
    assert "received" in stats
    assert "unique_processed" in stats
    assert "duplicate_dropped" in stats
    assert "topics" in stats
    assert "uptime_seconds" in stats
    assert stats["received"] == 0
    assert stats["unique_processed"] == 0

@pytest.mark.asyncio
async def test_get_stats_after_publish(http_client, cleanup_database):
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
    
    await http_client.post("/publish", json=events)
    
    response = await http_client.get("/stats")
    stats = response.json()
    
    assert stats["unique_processed"] == 10
    assert "stats_topic" in stats["topics"]

@pytest.mark.asyncio
async def test_health_check(http_client):
    """Test health check endpoint"""
    response = await http_client.get("/health")
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "healthy"
    assert "timestamp" in result

# ============================================================================
# TEST: BATCH OPERATIONS
# ============================================================================

@pytest.mark.asyncio
async def test_publish_batch_mixed_valid_invalid(http_client, cleanup_database):
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
    
    response = await http_client.post("/publish", json=events)
    result = response.json()
    
    assert result["accepted"] == 2
    assert result["processed"] == 2
    assert result["rejected"] == 1

@pytest.mark.asyncio
async def test_publish_large_batch(http_client, cleanup_database):
    """Test publishing large batch of events"""
    events = [
        {
            "topic": f"large_batch_{i % 5}",
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_publisher",
            "payload": {"index": i}
        }
        for i in range(500)
    ]
    
    response = await http_client.post("/publish", json=events)
    result = response.json()
    
    assert result["accepted"] == 500
    assert result["processed"] == 500
    
    # Verify in database
    stats_response = await http_client.get("/stats")
    stats = stats_response.json()
    assert stats["unique_processed"] >= 500

# ============================================================================
# TEST: ERROR HANDLING
# ============================================================================

@pytest.mark.asyncio
async def test_publish_empty_array(http_client, cleanup_database):
    """Test publish with empty event array"""
    response = await http_client.post("/publish", json=[])
    assert response.status_code == 400

@pytest.mark.asyncio
async def test_publish_not_array(http_client, cleanup_database):
    """Test publish with non-array body"""
    response = await http_client.post("/publish", json={"event_id": "test"})
    assert response.status_code == 400

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
