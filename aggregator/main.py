"""
Pub-Sub Log Aggregator Service
Sistem agregator terdistribusi dengan idempotent consumer dan deduplication
"""
import os
import json
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values
import asyncpg

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# GLOBAL STATE
# ============================================================================
class AppState:
    """Shared application state"""
    db_pool: Optional[asyncpg.pool.Pool] = None
    start_time: datetime = datetime.now(timezone.utc)
    received_count: int = 0
    unique_processed_count: int = 0
    duplicate_dropped_count: int = 0
    
    @classmethod
    async def init_db_pool(cls, dsn: str):
        """Initialize async database connection pool"""
        cls.db_pool = await asyncpg.create_pool(dsn)
        await cls._init_schema()
        logger.info("Database pool initialized")
    
    @classmethod
    async def close_db_pool(cls):
        """Close database connection pool"""
        if cls.db_pool:
            await cls.db_pool.close()
            logger.info("Database pool closed")
    
    @classmethod
    async def _init_schema(cls):
        """Initialize database schema"""
        if not cls.db_pool:
            raise RuntimeError("Database pool not initialized")
        
        async with cls.db_pool.acquire() as conn:
            # Create tables if not exist
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_event UNIQUE(topic, event_id)
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    id SERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_processed UNIQUE(topic, event_id)
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS event_log (
                    id SERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id SERIAL PRIMARY KEY,
                    metric_name TEXT NOT NULL UNIQUE,
                    metric_value BIGINT NOT NULL,
                    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            
            # Initialize stats counters
            await conn.execute("""
                INSERT INTO stats (metric_name, metric_value) 
                VALUES 
                    ('received_count', 0),
                    ('unique_processed_count', 0),
                    ('duplicate_dropped_count', 0)
                ON CONFLICT (metric_name) DO NOTHING;
            """)
            
            logger.info("Database schema initialized")

# ============================================================================
# LIFESPAN MANAGEMENT
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app startup and shutdown"""
    # Startup
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://user:password@localhost:5432/distributed_logs"
    )
    await AppState.init_db_pool(database_url)
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    await AppState.close_db_pool()
    logger.info("Application shutdown complete")

# ============================================================================
# FASTAPI APP
# ============================================================================
app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Distributed log aggregator dengan idempotency dan deduplication",
    version="1.0.0",
    lifespan=lifespan
)

# ============================================================================
# MODELS
# ============================================================================
class EventPayload(Dict[str, Any]):
    """Event payload as dictionary"""
    pass

class Event:
    """Event model"""
    def __init__(
        self,
        topic: str,
        event_id: str,
        timestamp: str,
        source: str,
        payload: dict
    ):
        self.topic = topic
        self.event_id = event_id
        self.timestamp = timestamp
        self.source = source
        self.payload = payload
    
    def validate(self) -> tuple[bool, Optional[str]]:
        """Validate event"""
        if not self.topic or not isinstance(self.topic, str):
            return False, "topic must be non-empty string"
        if not self.event_id or not isinstance(self.event_id, str):
            return False, "event_id must be non-empty string"
        if not self.timestamp:
            return False, "timestamp is required"
        if not self.source:
            return False, "source is required"
        if not isinstance(self.payload, dict):
            return False, "payload must be dict"
        return True, None
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "topic": self.topic,
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "source": self.source,
            "payload": self.payload
        }

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================
async def insert_event_atomic(event: Event) -> tuple[bool, str]:
    """
    Insert event with atomic deduplication using unique constraint.
    
    This implements idempotent INSERT pattern using constraint.
    On conflict (duplicate), return success without re-processing.
    
    Args:
        event: Event to insert
    
    Returns:
        (is_new: bool, message: str)
    """
    if not AppState.db_pool:
        raise RuntimeError("Database pool not initialized")
    
    try:
        async with AppState.db_pool.acquire() as conn:
            # Check if event already processed
            existing = await conn.fetchval(
                "SELECT 1 FROM processed_events WHERE topic = $1 AND event_id = $2",
                event.topic, event.event_id
            )
            
            if existing:
                logger.info(f"Duplicate detected: {event.topic}/{event.event_id}")
                await _log_event(conn, event.topic, event.event_id, "DUPLICATE", "Already processed")
                return False, "Event already processed (duplicate)"
            
            # Insert event (may conflict if simultaneous write)
            try:
                await conn.execute("""
                    INSERT INTO events (topic, event_id, timestamp, source, payload)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (topic, event_id) DO NOTHING;
                """, event.topic, event.event_id, event.timestamp, event.source, json.dumps(event.payload))
                
                # Mark as processed
                await conn.execute("""
                    INSERT INTO processed_events (topic, event_id)
                    VALUES ($1, $2)
                    ON CONFLICT (topic, event_id) DO NOTHING;
                """, event.topic, event.event_id)
                
                # Update counters atomically
                await conn.execute("""
                    UPDATE stats SET metric_value = metric_value + 1, last_updated = NOW()
                    WHERE metric_name = 'unique_processed_count';
                """)
                
                await _log_event(conn, event.topic, event.event_id, "PROCESSED", "Successfully inserted")
                logger.info(f"Processed new event: {event.topic}/{event.event_id}")
                return True, "Event processed successfully"
            
            except Exception as e:
                # Constraint violation on insert (race condition from concurrent write)
                logger.warning(f"Constraint violation (concurrent write): {event.topic}/{event.event_id} - {e}")
                
                # Check if it was processed anyway (race won by other transaction)
                check = await conn.fetchval(
                    "SELECT 1 FROM processed_events WHERE topic = $1 AND event_id = $2",
                    event.topic, event.event_id
                )
                if check:
                    await _log_event(conn, event.topic, event.event_id, "DUPLICATE", "Processed by concurrent transaction")
                    return False, "Event already processed by concurrent transaction"
                raise
    
    except Exception as e:
        logger.error(f"Error inserting event {event.topic}/{event.event_id}: {e}")
        raise

async def _log_event(
    conn: asyncpg.connection.Connection,
    topic: str,
    event_id: str,
    status: str,
    message: str
):
    """Log event processing status to audit log"""
    await conn.execute("""
        INSERT INTO event_log (topic, event_id, status, message)
        VALUES ($1, $2, $3, $4)
    """, topic, event_id, status, message)

async def increment_received_count():
    """Increment received event counter"""
    if not AppState.db_pool:
        return
    
    try:
        async with AppState.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE stats SET metric_value = metric_value + 1, last_updated = NOW()
                WHERE metric_name = 'received_count';
            """)
    except Exception as e:
        logger.error(f"Error updating received_count: {e}")

async def increment_duplicate_count():
    """Increment duplicate dropped counter"""
    if not AppState.db_pool:
        return
    
    try:
        async with AppState.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE stats SET metric_value = metric_value + 1, last_updated = NOW()
                WHERE metric_name = 'duplicate_dropped_count';
            """)
    except Exception as e:
        logger.error(f"Error updating duplicate_dropped_count: {e}")

async def get_stats() -> Dict[str, Any]:
    """Get aggregator statistics"""
    if not AppState.db_pool:
        return {
            "error": "Database not initialized",
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0,
            "topics": [],
            "uptime_seconds": 0
        }
    
    try:
        async with AppState.db_pool.acquire() as conn:
            # Get counters
            stats_row = await conn.fetch("SELECT metric_name, metric_value FROM stats")
            stats_dict = {row['metric_name']: row['metric_value'] for row in stats_row}
            
            # Get unique topics
            topics = await conn.fetch(
                "SELECT DISTINCT topic FROM events ORDER BY topic"
            )
            topic_list = [row['topic'] for row in topics]
            
            # Get event counts per topic
            topic_stats = await conn.fetch("""
                SELECT topic, COUNT(*) as count FROM events
                GROUP BY topic ORDER BY topic
            """)
            topic_count_dict = {row['topic']: row['count'] for row in topic_stats}
            
            uptime = (datetime.now(timezone.utc) - AppState.start_time).total_seconds()
            
            return {
                "received": stats_dict.get('received_count', 0),
                "unique_processed": stats_dict.get('unique_processed_count', 0),
                "duplicate_dropped": stats_dict.get('duplicate_dropped_count', 0),
                "topics": topic_list,
                "topic_event_counts": topic_count_dict,
                "uptime_seconds": int(uptime),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise

async def get_events(topic: Optional[str] = None, limit: int = 100) -> List[dict]:
    """Get events, optionally filtered by topic"""
    if not AppState.db_pool:
        return []
    
    try:
        async with AppState.db_pool.acquire() as conn:
            if topic:
                rows = await conn.fetch("""
                    SELECT topic, event_id, timestamp, source, payload, received_at
                    FROM events
                    WHERE topic = $1
                    ORDER BY received_at DESC
                    LIMIT $2
                """, topic, limit)
            else:
                rows = await conn.fetch("""
                    SELECT topic, event_id, timestamp, source, payload, received_at
                    FROM events
                    ORDER BY received_at DESC
                    LIMIT $1
                """, limit)
            
            return [
                {
                    "topic": row['topic'],
                    "event_id": row['event_id'],
                    "timestamp": row['timestamp'].isoformat() if row['timestamp'] else None,
                    "source": row['source'],
                    "payload": row['payload'],
                    "received_at": row['received_at'].isoformat() if row['received_at'] else None
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error fetching events: {e}")
        raise

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.post("/publish")
async def publish_events(
    events: List[dict],
    background_tasks: BackgroundTasks
) -> JSONResponse:
    """
    Publish one or more events.
    
    Request body: List of events with schema:
    {
        "topic": "string",
        "event_id": "string-unik",
        "timestamp": "ISO8601",
        "source": "string",
        "payload": { ... }
    }
    
    Returns: {
        "accepted": int,
        "processed": int,
        "rejected": int,
        "details": [...]
    }
    """
    logger.info(f"Received publish request with {len(events)} event(s)")
    
    if not isinstance(events, list):
        raise HTTPException(status_code=400, detail="Request body must be array of events")
    
    if len(events) == 0:
        raise HTTPException(status_code=400, detail="At least one event required")
    
    accepted = 0
    processed = 0
    rejected = 0
    details = []
    
    for idx, event_data in enumerate(events):
        try:
            # Validate event structure
            event = Event(
                topic=event_data.get("topic"),
                event_id=event_data.get("event_id"),
                timestamp=event_data.get("timestamp"),
                source=event_data.get("source"),
                payload=event_data.get("payload", {})
            )
            
            is_valid, error_msg = event.validate()
            if not is_valid:
                rejected += 1
                details.append({
                    "index": idx,
                    "event_id": event_data.get("event_id", "unknown"),
                    "status": "REJECTED",
                    "reason": error_msg
                })
                continue
            
            accepted += 1
            background_tasks.add_task(increment_received_count)
            
            # Try to process event
            is_new, message = await insert_event_atomic(event)
            
            if is_new:
                processed += 1
                details.append({
                    "index": idx,
                    "event_id": event.event_id,
                    "status": "PROCESSED",
                    "reason": message
                })
            else:
                background_tasks.add_task(increment_duplicate_count)
                details.append({
                    "index": idx,
                    "event_id": event.event_id,
                    "status": "DUPLICATE",
                    "reason": message
                })
        
        except Exception as e:
            rejected += 1
            details.append({
                "index": idx,
                "event_id": event_data.get("event_id", "unknown"),
                "status": "ERROR",
                "reason": str(e)
            })
            logger.error(f"Error processing event {idx}: {e}")
    
    logger.info(f"Publish result - accepted: {accepted}, processed: {processed}, rejected: {rejected}")
    
    return JSONResponse({
        "accepted": accepted,
        "processed": processed,
        "rejected": rejected,
        "details": details
    })

@app.get("/events")
async def get_events_endpoint(
    topic: Optional[str] = None,
    limit: int = 100
) -> JSONResponse:
    """
    Retrieve processed events.
    
    Query parameters:
    - topic: Filter by topic (optional)
    - limit: Max events to return (default: 100)
    
    Returns: {
        "count": int,
        "events": [...]
    }
    """
    try:
        events = await get_events(topic, limit)
        return JSONResponse({
            "count": len(events),
            "topic_filter": topic,
            "events": events
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching events: {str(e)}")

@app.get("/stats")
async def get_stats_endpoint() -> JSONResponse:
    """
    Get aggregator statistics.
    
    Returns: {
        "received": int,
        "unique_processed": int,
        "duplicate_dropped": int,
        "topics": [...],
        "topic_event_counts": {...},
        "uptime_seconds": int,
        "timestamp": "ISO8601"
    }
    """
    try:
        stats = await get_stats()
        return JSONResponse(stats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")

@app.get("/health")
async def health_check() -> JSONResponse:
    """Health check endpoint"""
    return JSONResponse({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("RELOAD", "false").lower() == "true"
    )
