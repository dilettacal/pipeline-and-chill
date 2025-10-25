# ChillFlow Core

Core foundation package for the ChillFlow data pipeline system.

## Package Structure

```
chillflow/
├── __init__.py              # Main package exports
├── settings.py              # Database, Redis, Kafka configuration
├── utils/                   # Utility functions
│   ├── hashing.py          # Hash functions for data anonymization
│   ├── logging.py          # Logging utilities for observability
│   ├── events.py           # Event handling utilities
│   └── models.py           # Common model utilities
├── models/                  # SQLAlchemy database models
├── schemas/                 # Pydantic data schemas
├── clients/                 # Database, Redis, Kafka clients
├── repositories/           # Base repository classes
└── migrations/             # Database migrations and seeding
    ├── alembic.ini         # Alembic configuration
    ├── seed_zones.py       # Database seeding script
    └── alembic/            # Migration files
```

## Features

- **Settings Management**: Environment-based configuration for database, Redis, and Kafka
- **Database Models**: SQLAlchemy models for trip data, zones, and analytics
- **Data Schemas**: Pydantic schemas for validation and serialization
- **Database Migrations**: Alembic-based schema management
- **Utility Functions**: Hashing, logging, and common helpers
- **Client Wrappers**: Database, Redis, and Kafka connection management

## Usage

### Basic Setup

```python
from chillflow import settings, generate_trip_key, get_logger
from chillflow.utils.logging import setup_development_logging

# Setup structured logging
setup_development_logging("my-service")

# Get logger
logger = get_logger("my-service")

# Generate trip key
trip_key = generate_trip_key("pickup", "dropoff", "datetime")

# Structured logging
logger.info("Trip processed", trip_key=trip_key, status="completed")

# Access settings
database_url = settings.DATABASE_URL
```

### Database Operations

```python
from chillflow import get_db_session, CompleteTrip, CompleteTripSchema

# Database session
with get_db_session() as session:
    # Query trips
    trips = session.query(CompleteTrip).limit(10).all()

    # Create trip from schema
    trip_data = CompleteTripSchema(
        trip_key="abc123",
        vendor_id=1,
        pickup_ts=datetime.now(),
        dropoff_ts=datetime.now(),
        pu_zone_id=229,
        do_zone_id=230,
        vehicle_id_h="hashed_vehicle_id"
    )

    # Convert to SQLAlchemy model
    trip = CompleteTrip(**trip_data.model_dump())
    session.add(trip)
```

### Redis Operations

```python
from chillflow import get_redis_client

# Redis client
redis = get_redis_client()

# Store data
redis.set("trip:abc123", "completed", ex=3600)

# Get data
status = redis.get("trip:abc123")

# JSON operations
redis.json_set("trip:data", {"trip_key": "abc123", "status": "completed"})
data = redis.json_get("trip:data")
```

### Kafka Operations

```python
from chillflow import get_kafka_client, IdentityEvent

# Kafka client
kafka = get_kafka_client()

# Producer
producer = kafka.get_producer()
producer.send_message("trip-events", {
    "trip_key": "abc123",
    "event_type": "IDENTITY",
    "status": "started"
})

# Consumer
consumer = kafka.get_consumer(
    topics=["trip-events"],
    group_id="trip-processor"
)
messages = consumer.consume_messages()
```

### Event Schemas

```python
from chillflow import IdentityEvent, EventType
from datetime import datetime

# Create identity event
event = IdentityEvent(
    trip_key="abc123",
    vehicle_id_h="hashed_vehicle",
    event_ts=datetime.now(),
    vendor_id=1,
    pickup_ts=datetime.now(),
    pu_zone_id=229,
    do_zone_id=230
)

# Validate event
print(event.event_type)  # EventType.IDENTITY
print(event.model_dump_json())
```

## Dependencies

- `pydantic` - Data validation and settings
- `sqlalchemy` - Database ORM
- `alembic` - Database migrations
- `redis` - Redis client
- `kafka-python` - Kafka client
- `structlog` - Structured logging framework
