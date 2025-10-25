# ChillFlow Stream Service

Real-time trip event processing for the ChillFlow data pipeline.

## Overview

The ChillFlow Stream Service handles real-time processing of taxi trip data by:

1. **Splitting complete trips into individual events** (trip-event-producer)
2. **Reassembling events into complete trips** (trip-assembler)
3. **Real-time analytics and monitoring**

## Architecture

```
Complete Trip → Trip Event Producer → Kafka Events → Trip Assembler → Complete Trip
     ↓              ↓                    ↓              ↓              ↓
[Pickup + Dropoff] → [Pickup Event] → [Kafka] → [Reassemble] → [Complete Trip]
                   → [Dropoff Event]
```

## Event Types

- **Trip Started**: When a trip begins
- **Trip Ended**: When a trip completes
- **Zone Entered/Exited**: Location-based events
- **Payment Processed**: Financial transaction events

## Usage

### CLI Commands

```bash
# Produce events from database trips
chillflow-stream produce-events --limit 100

# Produce events from specific trip
chillflow-stream produce-events --trip-key abc123

# Produce events from parquet file
chillflow-stream produce-from-file --file data/trips.parquet

# Consume and display events
chillflow-stream consume-events --topic trip-events

# Create Kafka topic
chillflow-stream create-topic --topic trip-events
```

### Python API

```python
from stream import TripEventProducer, TripStartedEvent

# Initialize producer
producer = TripEventProducer()

# Process a trip into events
events = producer.process_trip(complete_trip)

# Send events to Kafka
stats = producer.send_events(events, topic="trip-events")
```

## Installation

```bash
# Install the stream service
uv add chillflow-stream

# Or install in development mode
uv add --dev chillflow-stream
```

## Configuration

The stream service uses the following environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka servers (default: localhost:9092)
- `LOG_LEVEL`: Logging level (default: INFO)

## Development

```bash
# Run tests
make test stream

# Run linting
make lint

# Install in development mode
uv add --dev chillflow-stream
```
