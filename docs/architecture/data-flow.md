# Data Flow Architecture

## 🔄 Data Processing Pipeline

The ChillFlow pipeline processes NYC taxi data through multiple stages, combining batch and streaming processing for comprehensive analytics.

## 📊 Data Flow Diagram

```mermaid
graph LR
    %% Data Sources
    subgraph "Data Sources"
        RAW[("📁 Raw Data<br/>NYC Taxi Parquet")]
        ZONES[("🗺️ Zone Data<br/>Taxi Zones CSV")]
    end

    %% Batch Processing
    subgraph "Batch Processing"
        LOADER["📥 Data Loader<br/>Monthly Processing"]
        CURATOR["🔧 Data Curator<br/>Quality & Validation"]
        PRODUCER["🏭 Batch Producer<br/>Trip Generation"]
    end

    %% Streaming Processing
    subgraph "Streaming Processing"
        EVENT_PROD["📡 Event Producer<br/>Trip → Events"]
        KAFKA_STREAM[("⚡ Kafka Stream<br/>Event Bus")]
        ASSEMBLER["🔧 Trip Assembler<br/>Events → Trips"]
    end

    %% Storage
    subgraph "Storage Layer"
        POSTGRES[("🗄️ PostgreSQL<br/>Complete Trips")]
        REDIS_CACHE[("⚡ Redis<br/>Session Cache")]
    end

    %% Analytics
    subgraph "Analytics Layer"
        AGGREGATOR["📊 KPI Aggregator<br/>Zone Hourly Stats"]
        MART[("📈 Data Mart<br/>Analytics Tables")]
        REPORTS["📊 Reports<br/>Business Insights"]
    end

    %% Batch Processing Path
    RAW --> LOADER
    ZONES --> LOADER
    LOADER --> CURATOR
    CURATOR --> PRODUCER
    PRODUCER --> POSTGRES

    %% Streaming Processing Path
    POSTGRES --> EVENT_PROD
    EVENT_PROD --> KAFKA_STREAM
    KAFKA_STREAM --> ASSEMBLER
    ASSEMBLER --> POSTGRES

    %% Analytics Path
    POSTGRES --> AGGREGATOR
    AGGREGATOR --> MART
    MART --> REPORTS

    %% Caching
    POSTGRES -.-> REDIS_CACHE
    REDIS_CACHE -.-> AGGREGATOR

    %% Styling
    classDef source fill:#e1f5fe,stroke:#01579b,stroke-width:3px,color:#000
    classDef batch fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000
    classDef stream fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000
    classDef storage fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px,color:#000
    classDef analytics fill:#fce4ec,stroke:#880e4f,stroke-width:2px,color:#000

    class RAW,ZONES source
    class LOADER,CURATOR,PRODUCER batch
    class EVENT_PROD,KAFKA_STREAM,ASSEMBLER stream
    class POSTGRES,REDIS_CACHE storage
    class AGGREGATOR,MART,REPORTS analytics
```

## 🏗️ Processing Stages

### 1. **Data Ingestion**
- **Raw Data**: NYC taxi parquet files (monthly batches)
- **Reference Data**: Taxi zones, lookup tables
- **Quality Checks**: Data validation and cleaning

### 2. **Batch Processing**
- **Data Loader**: Processes monthly data files
- **Data Curator**: Validates and cleans raw data
- **Batch Producer**: Generates complete trip records
- **Storage**: Saves to PostgreSQL database

### 3. **Streaming Processing**
- **Event Producer**: Splits complete trips into granular events
- **Kafka Stream**: Real-time event streaming
- **Trip Assembler**: Reassembles events into complete trips
- **Storage**: Updates PostgreSQL with processed trips

### 4. **Analytics Processing**
- **KPI Aggregator**: Generates zone-hourly statistics
- **Data Mart**: Pre-computed analytics tables
- **Reports**: Business intelligence and insights

## 🔄 Event Types

### **Trip Events**
- **TripStartedEvent**: Trip initiation
- **TripEndedEvent**: Trip completion
- **PaymentProcessedEvent**: Payment processing
- **ZoneEvent**: Zone-specific events

### **Event Flow**
1. **Trip Started** → Kafka → Assembler
2. **Payment Processed** → Kafka → Assembler
3. **Trip Ended** → Kafka → Assembler
4. **Complete Trip** → Database → Analytics

## 📊 Data Schemas

### **Complete Trip Schema**
```python
class CompleteTrip:
    trip_id: str
    vendor_id: str
    pickup_ts: datetime
    dropoff_ts: datetime
    pickup_zone_id: int
    dropoff_zone_id: int
    passenger_count: int
    fare_amount: float
    tip_amount: float
    total_amount: float
    payment_type: str
```

### **Event Schemas**
```python
class TripStartedEvent:
    event_id: str
    trip_id: str
    vendor_id: str
    pickup_ts: datetime
    pickup_zone_id: int
    passenger_count: int

class TripEndedEvent:
    event_id: str
    trip_id: str
    dropoff_ts: datetime
    dropoff_zone_id: int
    fare_amount: float
    tip_amount: float
    total_amount: float
    payment_type: str
```

## 🚀 Performance Characteristics

### **Batch Processing**
- **Throughput**: ~10,000 trips/minute
- **Latency**: 5-10 minutes (monthly batches)
- **Storage**: PostgreSQL with optimized indexing

### **Streaming Processing**
- **Throughput**: ~1,000 events/second
- **Latency**: <100ms (real-time)
- **Storage**: Kafka + PostgreSQL

### **Analytics Processing**
- **Throughput**: ~100 KPIs/minute
- **Latency**: 1-5 minutes (hourly aggregation)
- **Storage**: Pre-computed data marts

## 🔧 Data Quality

### **Validation Rules**
- **Trip Duration**: 1 second to 24 hours
- **Fare Amount**: $0.01 to $500.00
- **Zone IDs**: Valid taxi zone references
- **Timestamps**: Chronological order

### **Quality Metrics**
- **Completeness**: >99% data coverage
- **Accuracy**: <0.1% validation errors
- **Consistency**: Cross-reference validation
- **Timeliness**: Real-time processing

## 📈 Monitoring & Observability

### **Key Metrics**
- **Data Volume**: Records processed per minute
- **Processing Latency**: End-to-end processing time
- **Error Rates**: Failed validations and processing
- **Resource Usage**: CPU, memory, disk I/O

### **Alerting**
- **Data Quality**: Validation failures
- **Processing Delays**: Latency thresholds
- **Resource Limits**: CPU/memory usage
- **System Health**: Service availability

## 🔄 Data Lineage

### **Source to Destination**
1. **Raw Data** → Data Loader → Curated Data
2. **Curated Data** → Batch Producer → Complete Trips
3. **Complete Trips** → Event Producer → Events
4. **Events** → Trip Assembler → Updated Trips
5. **Updated Trips** → KPI Aggregator → Analytics

### **Data Dependencies**
- **Zones** → Trip validation
- **Trips** → Event generation
- **Events** → Trip assembly
- **Trips** → KPI calculation
- **KPIs** → Report generation
