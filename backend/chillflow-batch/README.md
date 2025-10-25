# ChillFlow Batch Processing Service

Batch processing service for the ChillFlow data pipeline, providing data curation, trip processing, and aggregation capabilities.

## Features

- **Data Curation**: Transform raw parquet files to curated format with DQ rules
- **Trip Processing**: Process curated data and write to PostgreSQL
- **KPI Aggregation**: Compute hourly zone KPIs from trip data
- **Scheduling**: Automated batch job scheduling
- **Structured Logging**: Full structlog integration for observability

## Components

### BatchAggregator
Computes hourly zone KPIs from complete trip data:
- Full refresh aggregation
- Incremental aggregation
- Backfill capabilities
- KPI statistics and date range queries

### BatchTripProducer
Processes curated trip data and writes to database:
- Parquet file reading
- Trip key and vehicle ID generation
- Batch database writes
- Progress tracking

### MonthlyLoader
Data curation pipeline:
- Derived field computation (distance, duration, speed)
- Data quality rules (temporal, speed, amount validation)
- Column name standardization
- Parquet file output

### BatchScheduler
Automated job scheduling:
- Daily/hourly aggregation jobs
- Monthly processing jobs
- Job status monitoring
- Configurable schedules

## Usage

### CLI Commands

```bash
# Run aggregation
python -m chillflow aggregate run
python -m chillflow aggregate run --full-refresh

# Check aggregation status
python -m chillflow aggregate status

# Process trip data
python -m chillflow process trips /path/to/curated.parquet

# Curate raw data
python -m chillflow curate month /path/to/raw.parquet /path/to/curated.parquet

# Start scheduler
python -m chillflow schedule start

# View scheduled jobs
python -m chillflow schedule jobs
```

### Programmatic Usage

```python
from chillflow.batch import BatchAggregator, BatchTripProducer, MonthlyLoader

# Aggregation
aggregator = BatchAggregator()
stats = aggregator.aggregate_incremental(datetime.now())
print(f"Processed {stats['hours_processed']} hours")

# Trip Processing
producer = BatchTripProducer()
stats = producer.process_month(Path("data.parquet"))
print(f"Inserted {stats['inserted']} trips")

# Data Curation
loader = MonthlyLoader()
stats = loader.curate_month(Path("raw.parquet"), Path("curated.parquet"))
print(f"Retention rate: {stats['retention_rate']:.2f}%")
```

## Configuration

The service uses environment variables for configuration:

```bash
# Database
DATABASE_URL=postgresql://dev:dev@localhost:5432/chillflow

# Hashing
HASH_SALT=chillflow-dev-salt-2025

# Logging
LOG_LEVEL=INFO
```

## Testing

```bash
# Run unit tests
uv run pytest tests/ -m unit

# Run all tests
uv run pytest tests/

# Run with coverage
uv run pytest tests/ --cov=chillflow
```

## Dependencies

- `chillflow-core` - Core utilities and data models
- `pandas` - Data processing
- `pyarrow` - Parquet file handling
- `structlog` - Structured logging
- `click` - CLI interface
- `schedule` - Job scheduling

## Architecture

```
Raw Data → MonthlyLoader → Curated Data → BatchTripProducer → Database
                                                              ↓
Database → BatchAggregator → KPI Aggregations → Data Mart
```

The batch service processes data in stages:
1. **Curation**: Raw → Curated (DQ rules, derived fields)
2. **Processing**: Curated → Database (trip records)
3. **Aggregation**: Database → KPIs (hourly zone metrics)

## Logging

All components use structured logging with context:

```python
logger.info("Processing batch", file_name="data.parquet", rows=1000)
logger.error("Processing failed", error=str(e), file_name="data.parquet")
```

Logs include:
- Processing statistics
- Error context
- Performance metrics
- Data quality metrics
