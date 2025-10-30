# ChillFlow Pipeline 🚀

Chill and Learn with a cool event-driven data pipeline for batch and real-time analytics of NYC taxi data. Built with Python, Kafka, PostgreSQL, and cloud-native technologies.

## ✨ Key Features
- 🧩 Unified batch + streaming pipeline
- 🧠 Modular Python micro-packages
- 🧪 Tested (unit, integration, smoke)
- ☁️ Deployable locally (or to Azure/AWS with Terraform)
- 📊 Built-in KPI aggregation and analytics layer


## 🏗️ Architecture Overview

<!-- Source diagram: docs/diagrams/architecture.mmd -->
```mermaid
graph LR
    %% Data Sources
    subgraph "Data Sources"
        NYC[("🗽 NYC Taxi Data<br/>Parquet Files")]
        REF[("📋 Reference Data<br/>Zones, Lookups")]
    end

    %% Ingestion Layer
    subgraph "Ingestion Layer"
        BATCH["📥 Batch Processor<br/>Monthly Data Loading"]
        CURATE["🔧 Data Curation<br/>Quality Checks"]
    end

    %% Streaming Layer
    subgraph "Streaming Layer"
        PRODUCER["📡 Event Producer<br/>Trip → Events"]
        KAFKA(("⚡ Kafka<br/>Event Streaming"))
        ASSEMBLER["🔧 Trip Assembler<br/>Events → Trips"]
        REDIS[("⚡ Redis<br/>State Management")]
    end

    %% Storage Layer
    subgraph "Storage Layer"
        POSTGRES[("🗄️ PostgreSQL<br/>Complete Trips")]
    end

    %% Analytics Layer
    subgraph "Analytics Layer"
        AGGREGATOR["📊 KPI Aggregator<br/>Zone Hourly Stats"]
        REPORTS["📈 Reports<br/>Analytics & Insights"]
    end

    %% Infrastructure (commented out)
    %% subgraph "Infrastructure"
    %%     DOCKER["🐳 Docker<br/>Containerization"]
    %%     CI["🔄 GitHub Actions<br/>CI/CD Pipeline"]
    %%     TERRAFORM["🏗️ Terraform<br/>Infrastructure as Code"]
    %% end

    %% Data Flow (Batch)
    NYC -- raw parquet --> BATCH
    REF -- lookups --> BATCH
    BATCH -- curated parquet --> CURATE
    CURATE -- inserts --> POSTGRES

    %% Streaming Path (Events)
    NYC -- raw events --> PRODUCER
    PRODUCER -- trip events --> KAFKA
    KAFKA -- assembled trips --> ASSEMBLER
    ASSEMBLER <--> REDIS
    ASSEMBLER -- upserts --> POSTGRES

    %% Analytics Path
    POSTGRES --> AGGREGATOR
    AGGREGATOR --> REPORTS

    %% Infrastructure connections (commented out)
    %% DOCKER -.-> BATCH
    %% DOCKER -.-> PRODUCER
    %% DOCKER -.-> ASSEMBLER
    %% CI -.-> DOCKER
    %% TERRAFORM -.-> DOCKER

    %% Styling
    classDef dataSource fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000
    classDef processing fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000
    classDef storage fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px,color:#000
    classDef analytics fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000
    classDef k8s stroke:#2962ff,stroke-width:3px,stroke-dasharray: 5 3
    %% classDef infrastructure fill:#fce4ec,stroke:#880e4f,stroke-width:2px,color:#000

    class NYC,REF dataSource
    class BATCH,CURATE,PRODUCER,ASSEMBLER,REDIS processing
    class POSTGRES,KAFKA storage
    class AGGREGATOR,REPORTS analytics

    %% Kubernetes (containerized workloads)
    class BATCH,CURATE,PRODUCER,ASSEMBLER,AGGREGATOR k8s
    class KAFKA,POSTGRES,REDIS k8s

    %% Legend
    subgraph "Legend"
        L_DS["Data Source"]:::dataSource
        L_PROC["Processing Service"]:::processing
        L_STORE["Storage"]:::storage
        L_ANALYTICS["Analytics"]:::analytics
        L_K8S["Kubernetes-managed (dashed blue border)"]:::processing
        L_KAFKA(("Kafka (event bus)"))
    end
    class L_K8S k8s
    class L_KAFKA k8s
    %% class DOCKER,CI,TERRAFORM infrastructure
```

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- Docker & Docker Compose
- uv (Python package manager)

### Local Development
```bash
# Clone the repository
git clone <repository-url>
cd pipeline-and-chill

# Install dependencies
uv sync

# Start infrastructure
make env up

# Run the complete pipeline
make pipeline full

# Run tests
make test all
```

## 🏗️ Technology Stack

```mermaid
graph TB
    %% Frontend/API Layer
    subgraph "API Layer"
        FASTAPI["⚡ FastAPI<br/>REST API"]
        CLICK["🖱️ Click<br/>CLI Interface"]
    end

    %% Backend Services
    subgraph "Backend Services"
        PYTHON["🐍 Python 3.12<br/>Core Language"]
        PYDANTIC["📋 Pydantic v2<br/>Data Validation"]
        SQLALCHEMY["🗄️ SQLAlchemy<br/>ORM"]
        ALEMBIC["🔄 Alembic<br/>DB Migrations"]
    end

    %% Streaming & Messaging
    subgraph "Streaming & Messaging"
        KAFKA["⚡ Apache Kafka<br/>Event Streaming"]
        TESTCONTAINERS["🐳 Testcontainers<br/>Integration Testing"]
        REDIS["⚡ Redis<br/>Caching & Sessions"]
    end

    %% Data & Storage
    subgraph "Data & Storage"
        POSTGRES["🐘 PostgreSQL<br/>Primary Database"]
        PARQUET["📊 Parquet<br/>Data Files"]
        PANDAS["🐼 Pandas<br/>Data Processing"]
    end

    %% DevOps & Infrastructure
    subgraph "DevOps & Infrastructure"
        DOCKER["🐳 Docker<br/>Containerization"]
        GITHUB["🔄 GitHub Actions<br/>CI/CD"]
        TERRAFORM["🏗️ Terraform<br/>Infrastructure"]
        UV["⚡ uv<br/>Package Management"]
    end

    %% Testing & Quality
    subgraph "Testing & Quality"
        PYTEST["🧪 pytest<br/>Testing Framework"]
        PRECOMMIT["✅ pre-commit<br/>Code Quality"]
        BLACK["🎨 Black<br/>Code Formatting"]
        FLAKE8["🔍 flake8<br/>Linting"]
    end

    %% Connections
    PYTHON --> PYDANTIC
    PYTHON --> SQLALCHEMY
    PYTHON --> FASTAPI
    PYTHON --> CLICK
    SQLALCHEMY --> ALEMBIC
    SQLALCHEMY --> POSTGRES
    PYDANTIC --> KAFKA
    KAFKA --> TESTCONTAINERS
    PYTHON --> PANDAS
    PANDAS --> PARQUET
    PYTHON --> PYTEST
    PYTEST --> TESTCONTAINERS
    PYTHON --> PRECOMMIT
    PRECOMMIT --> BLACK
    PRECOMMIT --> FLAKE8
    DOCKER --> GITHUB
    TERRAFORM --> DOCKER
    UV --> PYTHON

    %% Styling
    classDef language fill:#ffebee,stroke:#c62828,stroke-width:3px,color:#000
    classDef framework fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px,color:#000
    classDef database fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#000
    classDef streaming fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#000
    classDef devops fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000
    classDef testing fill:#e0f2f1,stroke:#00695c,stroke-width:2px,color:#000

    class PYTHON language
    class FASTAPI,CLICK,PYDANTIC,SQLALCHEMY,ALEMBIC,PANDAS framework
    class POSTGRES,PARQUET,REDIS database
    class KAFKA,TESTCONTAINERS streaming
    class DOCKER,GITHUB,TERRAFORM,UV devops
    class PYTEST,PRECOMMIT,BLACK,FLAKE8 testing
```

## 🧪 Testing Strategy

```mermaid
graph TB
    %% Test Pyramid
    subgraph "Test Pyramid"
        UNIT["🧪 Unit Tests<br/>Fast, Isolated<br/>~100ms each"]
        INTEGRATION["🔗 Integration Tests<br/>Real Services<br/>~5-10s each"]
        E2E["🌐 End-to-End Tests<br/>Full Pipeline<br/>~30-60s each"]
    end

    %% Test Types
    subgraph "Test Categories"
        STREAM_TESTS["⚡ Stream Tests<br/>Kafka Integration"]
        BATCH_TESTS["📊 Batch Tests<br/>PostgreSQL Integration"]
        CONTRACT_TESTS["📋 Contract Tests<br/>Schema Validation"]
        SMOKE_TESTS["💨 Smoke Tests<br/>CLI Validation"]
        PERFORMANCE["⚡ Performance Tests<br/>Benchmarks"]
    end

    %% Test Infrastructure
    subgraph "Test Infrastructure"
        TESTCONTAINERS["🐳 Testcontainers<br/>Real Services"]
        POSTGRES_TEST[("🗄️ PostgreSQL<br/>Test Database")]
        KAFKA_TEST[("⚡ Kafka<br/>Test Broker")]
        REDIS_TEST[("⚡ Redis<br/>Test Cache")]
    end

    %% Quality Gates
    subgraph "Quality Gates"
        PRECOMMIT["✅ pre-commit<br/>Code Quality"]
        LINTING["🔍 Linting<br/>flake8, isort"]
        FORMATTING["🎨 Formatting<br/>Black"]
        TYPE_CHECK["📝 Type Checking<br/>mypy"]
    end

    %% CI/CD Integration
    subgraph "CI/CD Integration"
        GITHUB_ACTIONS["🔄 GitHub Actions<br/>Automated Testing"]
        PARALLEL["⚡ Parallel Execution<br/>7 Test Jobs"]
        MATRIX["📊 Matrix Strategy<br/>Efficient Resource Usage"]
    end

    %% Test Flow
    UNIT --> INTEGRATION
    INTEGRATION --> E2E

    STREAM_TESTS --> TESTCONTAINERS
    BATCH_TESTS --> TESTCONTAINERS
    TESTCONTAINERS --> POSTGRES_TEST
    TESTCONTAINERS --> KAFKA_TEST
    TESTCONTAINERS --> REDIS_TEST

    PRECOMMIT --> LINTING
    PRECOMMIT --> FORMATTING
    PRECOMMIT --> TYPE_CHECK

    GITHUB_ACTIONS --> PARALLEL
    PARALLEL --> MATRIX
    MATRIX --> STREAM_TESTS
    MATRIX --> BATCH_TESTS
    MATRIX --> CONTRACT_TESTS
    MATRIX --> SMOKE_TESTS

    %% Styling
    classDef pyramid fill:#e8f5e8,stroke:#2e7d32,stroke-width:3px,color:#000
    classDef categories fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#000
    classDef infrastructure fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#000
    classDef quality fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000
    classDef cicd fill:#e0f2f1,stroke:#00695c,stroke-width:2px,color:#000

    class UNIT,INTEGRATION,E2E pyramid
    class STREAM_TESTS,BATCH_TESTS,CONTRACT_TESTS,SMOKE_TESTS,PERFORMANCE categories
    class TESTCONTAINERS,POSTGRES_TEST,KAFKA_TEST,REDIS_TEST infrastructure
    class PRECOMMIT,LINTING,FORMATTING,TYPE_CHECK quality
    class GITHUB_ACTIONS,PARALLEL,MATRIX cicd
```

## 🔄 CI/CD Pipeline

```mermaid
graph TB
    %% Trigger
    TRIGGER["🚀 Push/PR Trigger"]

    %% Parallel Jobs
    subgraph "Parallel Test Execution"
        UNIT["🧪 Unit Tests<br/>Fast, Isolated"]
        STREAM["⚡ Stream Tests<br/>Kafka Integration"]
        BATCH["📊 Batch Tests<br/>PostgreSQL Integration"]
        CONTRACT["📋 Contract Tests<br/>Schema Validation"]
        SMOKE["💨 Smoke Tests<br/>CLI Validation"]
        QUALITY["✅ Quality Checks<br/>Linting & Formatting"]
    end

    %% Integration Tests
    subgraph "Integration Tests"
        STREAM_INT["⚡ Stream Integration<br/>Testcontainers + Kafka"]
        BATCH_INT["📊 Batch Integration<br/>Testcontainers + PostgreSQL"]
    end

    %% Infrastructure
    subgraph "Infrastructure"
        DOCKER["🐳 Docker<br/>Container Runtime"]
        TESTCONTAINERS["🧪 Testcontainers<br/>Real Services"]
        REDPANDA["⚡ Redpanda<br/>Kafka Alternative"]
    end

    %% Quality Gates
    subgraph "Quality Gates"
        PRECOMMIT["✅ pre-commit<br/>Code Quality"]
        BLACK["🎨 Black<br/>Code Formatting"]
        ISORT["📦 isort<br/>Import Sorting"]
        FLAKE8["🔍 flake8<br/>Linting"]
    end

    %% Results
    SUCCESS["✅ All Tests Pass"]
    FAILURE["❌ Test Failure"]
    DEPLOY["🚀 Ready for Deployment"]

    %% Flow
    TRIGGER --> UNIT
    TRIGGER --> STREAM
    TRIGGER --> BATCH
    TRIGGER --> CONTRACT
    TRIGGER --> SMOKE
    TRIGGER --> QUALITY

    UNIT --> STREAM_INT
    STREAM --> STREAM_INT
    BATCH --> BATCH_INT

    STREAM_INT --> DOCKER
    BATCH_INT --> DOCKER
    DOCKER --> TESTCONTAINERS
    TESTCONTAINERS --> REDPANDA

    QUALITY --> PRECOMMIT
    PRECOMMIT --> BLACK
    PRECOMMIT --> ISORT
    PRECOMMIT --> FLAKE8

    STREAM_INT --> SUCCESS
    BATCH_INT --> SUCCESS
    CONTRACT --> SUCCESS
    SMOKE --> SUCCESS
    PRECOMMIT --> SUCCESS

    SUCCESS --> DEPLOY
    FAILURE --> TRIGGER

    %% Styling
    classDef trigger fill:#ffebee,stroke:#c62828,stroke-width:3px,color:#000
    classDef test fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px,color:#000
    classDef integration fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#000
    classDef infrastructure fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#000
    classDef quality fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#000
    classDef result fill:#e0f2f1,stroke:#00695c,stroke-width:3px,color:#000

    class TRIGGER trigger
    class UNIT,STREAM,BATCH,CONTRACT,SMOKE test
    class STREAM_INT,BATCH_INT integration
    class DOCKER,TESTCONTAINERS,REDPANDA infrastructure
    class QUALITY,PRECOMMIT,BLACK,ISORT,FLAKE8 quality
    class SUCCESS,FAILURE,DEPLOY result
```

## 📁 Project Structure

```
pipeline-and-chill/
├── backend/
│   ├── chillflow-core/          # Shared core components
│   ├── chillflow-batch/         # Batch processing service
│   └── chillflow-stream/        # Streaming service
├── docs/                        # Documentation
├── infra/                       # Infrastructure as Code
├── tests/                       # Test suites
└── scripts/                     # Utility scripts
```

## 🚀 Available Commands

### Environment Management
```bash
# Start infrastructure (PostgreSQL, Redis, Kafka)
make env up

# Check infrastructure status
make env status

# Stop infrastructure
make env down

# Clean up everything
make env clean
```

### Pipeline Operations
```bash
# Show available pipeline types
make pipeline

# Run specific pipeline stages
make pipeline ingestion    # Data ingestion
make pipeline batch       # Batch processing
make pipeline stream      # Streaming pipeline
make pipeline analytics   # Analytics & reports
make pipeline full        # Complete end-to-end

# Use custom year/month
YEAR=2025 MONTH=02 make pipeline batch
```

### Streaming Pipeline (2-Terminal Setup)
```bash
# Terminal 1: Start event producer
make stream produce

# Terminal 2: Start trip assembler
make stream assemble

# Or use automated streaming
make pipeline stream
```

### Testing
```bash
# Show available test types
make test

# Run all tests
make test all

# Run specific test suites
make test unit           # Unit tests
make test stream         # Stream service tests
make test batch          # Batch service tests
make test integration    # Integration tests
make test infra          # Infrastructure tests
```

### Quality & Linting
```bash
# Show available lint operations
make lint

# Run pre-commit hooks
make lint check

# Auto-fix issues
make lint fix

# Update pre-commit hooks
make lint update
```

### Database Operations
```bash
# Clean database
make db clean

# Connect to PostgreSQL
make db shell
```

#### Queries

```sql
-- Check for duplicate trip_key (should be 0 due to PK)
SELECT trip_key, COUNT(*) AS cnt
FROM stg.complete_trip
GROUP BY trip_key
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;
```

```sql
-- Validate NOT NULL columns for unexpected NULLs
SELECT
  SUM(CASE WHEN trip_key IS NULL THEN 1 ELSE 0 END) AS null_trip_key,
  SUM(CASE WHEN vendor_id IS NULL THEN 1 ELSE 0 END) AS null_vendor_id,
  SUM(CASE WHEN pickup_ts IS NULL THEN 1 ELSE 0 END) AS null_pickup_ts,
  SUM(CASE WHEN dropoff_ts IS NULL THEN 1 ELSE 0 END) AS null_dropoff_ts,
  SUM(CASE WHEN pu_zone_id IS NULL THEN 1 ELSE 0 END) AS null_pu_zone_id,
  SUM(CASE WHEN do_zone_id IS NULL THEN 1 ELSE 0 END) AS null_do_zone_id,
  SUM(CASE WHEN vehicle_id_h IS NULL THEN 1 ELSE 0 END) AS null_vehicle_id_h
FROM stg.complete_trip;
```

```sql
-- Spot potential duplicate-generation sources
SELECT vendor_id, pickup_ts, pu_zone_id, COUNT(*) AS cnt
FROM stg.complete_trip
GROUP BY vendor_id, pickup_ts, pu_zone_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;
```

```sql
-- Sample a specific trip_key (replace value)
SELECT *
FROM stg.complete_trip
WHERE trip_key = 'REPLACE_WITH_TRIP_KEY';
```

```sql
-- Validate foreign key references to zones
SELECT COUNT(*) AS invalid_pu_zone_refs
FROM stg.complete_trip t
LEFT JOIN dim.zone z ON t.pu_zone_id = z.zone_id
WHERE z.zone_id IS NULL;

SELECT COUNT(*) AS invalid_do_zone_refs
FROM stg.complete_trip t
LEFT JOIN dim.zone z ON t.do_zone_id = z.zone_id
WHERE z.zone_id IS NULL;
```

```sql
-- Check presence of audit/model columns
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'stg' AND table_name = 'complete_trip'
  AND column_name IN ('last_update_ts', 'source', 'created_at', 'updated_at')
ORDER BY column_name;
```

```sql
-- Throughput by hour
SELECT DATE_TRUNC('hour', pickup_ts) AS hour_bucket, COUNT(*) AS trips
FROM stg.complete_trip
GROUP BY 1
ORDER BY 1 DESC
LIMIT 48;
```

```sql
-- Payment-type distribution and card share
SELECT payment_type, COUNT(*) AS cnt
FROM stg.complete_trip
GROUP BY payment_type
ORDER BY cnt DESC;

SELECT
  100.0 * SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS pct_card
FROM stg.complete_trip;
```

```sql
-- Upsert smoke test (safe in a transaction)
BEGIN;
INSERT INTO stg.complete_trip (
  trip_key, vendor_id, pickup_ts, dropoff_ts, pu_zone_id, do_zone_id, vehicle_id_h, source, last_update_ts
) VALUES (
  'TEST_TRIP_KEY', 1, NOW(), NOW() + INTERVAL '10 min', 229, 230, 'veh_test', 'manual', NOW()
)
ON CONFLICT (trip_key) DO UPDATE SET
  vendor_id = EXCLUDED.vendor_id,
  pickup_ts = EXCLUDED.pickup_ts,
  dropoff_ts = EXCLUDED.dropoff_ts,
  pu_zone_id = EXCLUDED.pu_zone_id,
  do_zone_id = EXCLUDED.do_zone_id,
  vehicle_id_h = EXCLUDED.vehicle_id_h,
  source = EXCLUDED.source,
  last_update_ts = NOW();

SELECT * FROM stg.complete_trip WHERE trip_key = 'TEST_TRIP_KEY';
ROLLBACK;
```

```sql
-- Verify current transaction/session state
SELECT txid_current(), NOW();
```

## 📚 Documentation

- [System Architecture](docs/architecture/system-overview.md)
- [Data Flow](docs/architecture/data-flow.md)
- [Testing Strategy](docs/architecture/testing-strategy.md)
- [Quick Start Guide](docs/deployment/quick-start.md)


## 📄 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Attribution
This project uses public NYC Yellow Taxi Trip Data made available by the NYC Taxi and Limousine Commission via [NYC Open Data](https://data.cityofnewyork.us/Transportation/Yellow-Taxi-Trip-Records/).
