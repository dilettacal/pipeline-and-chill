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

## Dependencies

- `pydantic` - Data validation and settings
- `sqlalchemy` - Database ORM
- `alembic` - Database migrations
- `redis` - Redis client
- `kafka-python` - Kafka client
- `structlog` - Structured logging framework

