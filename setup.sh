#!/bin/bash
# Quick setup and run script for the distributed log aggregator

set -e

echo "=== Pub-Sub Log Aggregator Setup ==="

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Step 1: Building Docker images...${NC}"
docker compose build

echo -e "${BLUE}Step 2: Starting services...${NC}"
docker compose up -d

echo -e "${BLUE}Step 3: Waiting for services to be ready...${NC}"
sleep 10

# Check health
echo -e "${BLUE}Step 4: Verifying services...${NC}"
MAX_RETRIES=30
RETRY=0

until curl -f http://localhost:8080/health 2>/dev/null || [ $RETRY -eq $MAX_RETRIES ]; do
  echo "Waiting for aggregator... ($RETRY/$MAX_RETRIES)"
  sleep 1
  RETRY=$((RETRY + 1))
done

if [ $RETRY -eq $MAX_RETRIES ]; then
  echo "❌ Aggregator not ready"
  docker compose logs aggregator
  exit 1
fi

echo -e "${GREEN}✓ Aggregator is healthy${NC}"

# Check database
RETRY=0
until docker exec uas-storage psql -U user -d distributed_logs -c "SELECT 1" 2>/dev/null || [ $RETRY -eq $MAX_RETRIES ]; do
  echo "Waiting for database... ($RETRY/$MAX_RETRIES)"
  sleep 1
  RETRY=$((RETRY + 1))
done

if [ $RETRY -eq $MAX_RETRIES ]; then
  echo "❌ Database not ready"
  exit 1
fi

echo -e "${GREEN}✓ Database is ready${NC}"

echo -e "${BLUE}Step 5: Checking initial stats...${NC}"
curl -s http://localhost:8080/stats | jq .

echo ""
echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo "Available endpoints:"
echo "  • API: http://localhost:8080"
echo "  • Health: http://localhost:8080/health"
echo "  • Stats: http://localhost:8080/stats"
echo "  • Events: http://localhost:8080/events"
echo ""
echo "Commands:"
echo "  • View logs: docker compose logs -f"
echo "  • Stop: docker compose down"
echo "  • Clean: docker compose down && docker volume rm uas_pg_data uas_aggregator_data"
echo ""
