# Database Architecture

## Schemas

- dim: Reference/lookup data (slowly changing, shared across facts)
- stg: Canonical complete trips (system-of-record for processed facts)
- mart: Aggregated analytics tables optimized for BI

Rationale: Clear separation of concerns, aligning to warehouse best practices. `dim` enforces FK integrity, `stg` supports idempotent upserts from batch/stream, `mart` serves fast analytics without burdening operational reads/writes.

## Core Tables

### dim.zone
- Purpose: Taxi zone lookup
- PK: zone_id
- Columns: zone_id, borough, zone_name, service_zone, created_at, updated_at

### stg.complete_trip
- Purpose: Unified “complete trip” fact (from batch parquet or streaming events)
- PK: trip_key (deterministic SHA-256; enables idempotent writes)
- FKs: (pu_zone_id → dim.zone.zone_id), (do_zone_id → dim.zone.zone_id)
- Columns (selected): vendor_id, pickup_ts, dropoff_ts, pu_zone_id, do_zone_id, passenger_count, trip_distance, fare_amount, tip_amount, total_amount, payment_type, vehicle_id_h, last_update_ts, source
- Indexes: pickup_ts, pu_zone_id, do_zone_id, vendor_id (see migrations)

### mart.zone_hourly_kpis
- Purpose: Aggregated hourly metrics by pickup zone
- Grain: (zone_id, hour_ts)
- Columns (selected): trips, avg_fare, avg_tip, avg_speed_kmh, avg_distance_km, avg_duration_min, pct_card, unique_vehicles, created_at

## Modeling Style

- Closest to star schema:
  - Fact: stg.complete_trip (two role-playing keys to zone)
  - Dimension: dim.zone
  - Aggregated fact: mart.zone_hourly_kpis
- Not 3NF: facts are intentionally denormalized for analytics simplicity.
- Not snowflake: no chained normalized dimensions.

## Write Patterns

- Batch: curated monthly parquet → stg.complete_trip (idempotent upsert)
- Streaming: events → assembler (Redis for state) → stg.complete_trip (upsert)
- Aggregation: read stg → upsert mart.zone_hourly_kpis

Upsert guidance:
- Conflict target: (trip_key) on stg.complete_trip
- Update mutable fields and touch timestamps on conflict

## Naming Conventions

- Schemas: dim, stg, mart
- Facts: action_subject_grain (e.g., zone_hourly_kpis)
- Dimensions: business entity singular (e.g., zone)
- Keys: *_id for natural/surrogate; *_key for hashed/business keys

## Example Queries

```sql
-- Row counts
SELECT COUNT(*) AS total_rows FROM stg.complete_trip;

-- Recent trips
SELECT * FROM stg.complete_trip
ORDER BY pickup_ts DESC
LIMIT 10;

-- Duplicate detection safeguard (should be zero)
SELECT trip_key, COUNT(*) AS cnt
FROM stg.complete_trip
GROUP BY trip_key
HAVING COUNT(*) > 1;

-- FK integrity
SELECT COUNT(*) AS invalid_pu
FROM stg.complete_trip t
LEFT JOIN dim.zone z ON z.zone_id = t.pu_zone_id
WHERE z.zone_id IS NULL;
```

```sql
-- KPI sample
SELECT * FROM mart.zone_hourly_kpis
ORDER BY hour_ts DESC, zone_id
LIMIT 20;
```

## Maintenance

- Seeding: dim.zone via seed_zones.py (see migrations README)
- Migrations: Alembic under backend/chillflow-core/core/migrations
- Retention: consider archiving old stg partitions if volume grows
- Performance: ensure indexes from migrations are present; add covering indexes if query patterns expand
