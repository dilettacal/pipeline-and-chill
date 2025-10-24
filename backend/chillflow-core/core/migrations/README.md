# ChillFlow Database Migrations

This directory contains Alembic database migrations for the ChillFlow project.

## Structure

- `alembic.ini` - Alembic configuration
- `alembic/` - Migration scripts directory
  - `env.py` - Alembic environment configuration
  - `versions/` - Migration files
- `seed_zones.py` - Database seeding script for taxi zones

## Usage

### Run Migrations

```bash
# From the migrations directory
cd backend/chillflow-core/chillflow/migrations

# Run all pending migrations
uv run alembic upgrade head

# Create a new migration
uv run alembic revision --autogenerate -m "Description of changes"

# Rollback last migration
uv run alembic downgrade -1
```

### Seed Database

```bash
# Seed taxi zones data
uv run python seed_zones.py
```

## Database Schema

The initial schema includes:

- **`dim.zone`** - Taxi zone lookup table
- **`stg.complete_trip`** - Staging table for complete trip records
- **`mart.*`** - Analytics tables (to be added)

## Environment Variables

- `DATABASE_URL` - PostgreSQL connection string
- `HASH_SALT` - Salt for data anonymization
