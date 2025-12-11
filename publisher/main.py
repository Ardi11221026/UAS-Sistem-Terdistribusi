"""
Event Publisher Service
Generator event untuk aggregator dengan kontrol duplikasi
"""
import os
import json
import uuid
import asyncio
import random
import logging
from datetime import datetime, timezone
from typing import List, Optional

import httpx
import asyncpg

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================
DEFAULT_TOPICS = ["logs", "metrics", "audit", "events", "security"]
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8080")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "1000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.3"))  # 30% duplikasi

# ============================================================================
# EVENT GENERATION
# ============================================================================
class EventGenerator:
    """Generate and publish events"""
    
    def __init__(self, aggregator_url: str):
        self.aggregator_url = aggregator_url
        self.generated_events: dict = {}  # topic -> [event_ids]
        self.published_count = 0
        self.duplicate_count = 0
    
    def generate_event(self, topic: str, force_duplicate: bool = False) -> dict:
        """
        Generate single event.
        
        Args:
            topic: Event topic
            force_duplicate: If True, return duplicate of previously generated event
        
        Returns:
            Event dict
        """
        if force_duplicate and topic in self.generated_events and self.generated_events[topic]:
            # Re-use previously generated event_id
            event_id = random.choice(self.generated_events[topic])
            logger.debug(f"Generating duplicate event: {topic}/{event_id}")
            self.duplicate_count += 1
        else:
            # Generate new event_id
            event_id = str(uuid.uuid4())
            if topic not in self.generated_events:
                self.generated_events[topic] = []
            self.generated_events[topic].append(event_id)
        
        return {
            "topic": topic,
            "event_id": event_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": f"publisher-{os.getenv('HOSTNAME', 'unknown')}",
            "payload": {
                "message": f"Event from publisher",
                "sequence": self.published_count,
                "random_value": random.random(),
                "tags": random.sample(["tag1", "tag2", "tag3", "tag4"], k=2)
            }
        }
    
    def generate_batch(self, count: int) -> List[dict]:
        """
        Generate batch of events with controlled duplication.
        
        Args:
            count: Number of events to generate
        
        Returns:
            List of events
        """
        events = []
        for i in range(count):
            # Introduce duplicates based on DUPLICATE_RATE
            should_duplicate = random.random() < DUPLICATE_RATE
            topic = random.choice(DEFAULT_TOPICS)
            event = self.generate_event(topic, force_duplicate=should_duplicate)
            events.append(event)
            self.published_count += 1
        
        return events
    
    async def publish_batch(self, events: List[dict]) -> bool:
        """
        Publish batch of events to aggregator.
        
        Args:
            events: List of events to publish
        
        Returns:
            True if successful
        """
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.aggregator_url}/publish",
                    json=events,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(
                        f"Batch published: {len(events)} events → "
                        f"accepted={result['accepted']}, "
                        f"processed={result['processed']}, "
                        f"rejected={result['rejected']}"
                    )
                    return True
                else:
                    logger.error(f"Publish failed: {response.status_code} - {response.text}")
                    return False
        
        except Exception as e:
            logger.error(f"Error publishing batch: {e}")
            return False
    
    async def run_simulation(self):
        """
        Run event generation and publishing simulation.
        """
        logger.info(f"Starting simulation: {TOTAL_EVENTS} events, {DUPLICATE_RATE*100}% duplicates")
        
        batches = (TOTAL_EVENTS + BATCH_SIZE - 1) // BATCH_SIZE
        
        for batch_num in range(batches):
            batch_size = min(BATCH_SIZE, TOTAL_EVENTS - batch_num * BATCH_SIZE)
            
            logger.info(f"Generating batch {batch_num + 1}/{batches} ({batch_size} events)")
            events = self.generate_batch(batch_size)
            
            success = await self.publish_batch(events)
            if not success:
                logger.warning(f"Batch {batch_num + 1} publish failed, retrying...")
                await asyncio.sleep(2)
                success = await self.publish_batch(events)
            
            if success:
                await asyncio.sleep(0.5)  # Rate limiting
            else:
                logger.error(f"Batch {batch_num + 1} failed after retry")
        
        logger.info(
            f"Simulation complete: published={self.published_count}, "
            f"duplicates_injected={self.duplicate_count}"
        )

# ============================================================================
# HEALTH CHECK
# ============================================================================
async def health_check_loop():
    """Periodic health check of aggregator"""
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.aggregator_url}/health")
                if response.status_code == 200:
                    logger.debug("Aggregator health: OK")
        except Exception as e:
            logger.warning(f"Aggregator health check failed: {e}")
        
        await asyncio.sleep(10)

# ============================================================================
# MAIN
# ============================================================================
async def main():
    """Main entry point"""
    logger.info("Event Publisher starting...")
    
    generator = EventGenerator(AGGREGATOR_URL)
    
    # Wait for aggregator to be ready
    max_retries = 30
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{AGGREGATOR_URL}/health")
                if response.status_code == 200:
                    logger.info("Aggregator is ready")
                    break
        except Exception as e:
            logger.info(f"Waiting for aggregator... ({attempt + 1}/{max_retries})")
            await asyncio.sleep(1)
    else:
        logger.error("Aggregator not ready after 30 attempts")
        return
    
    # Run simulation
    await generator.run_simulation()
    
    logger.info("Event Publisher finished")

if __name__ == "__main__":
    asyncio.run(main())
