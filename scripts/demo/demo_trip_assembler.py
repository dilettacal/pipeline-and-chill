#!/usr/bin/env python3
"""
Demo script for Trip Assembler.

Shows how the assembler consumes events from Kafka, assembles trips in Redis,
and writes complete trips to PostgreSQL.

This demo:
1. Checks that infrastructure is running (Kafka, Redis, Postgres)
2. Sends sample events to Kafka manually
3. Runs the assembler for a few seconds
4. Shows results in the database

Usage:
    python scripts/demo_trip_assembler.py

    # Or via make:
    make demo-assembler
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "stream-trip-assembler"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "core-utils"))

import redis
from fluxframe.settings import settings
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from loguru import logger
from sqlalchemy import create_engine, text


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def check_kafka():
    """Check if Kafka is accessible."""
    print_section("1. Checking Kafka Connection")
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=5000
        )
        producer.close()
        print(f"✅ Kafka is running at {settings.KAFKA_BOOTSTRAP_SERVERS}")
        return True
    except NoBrokersAvailable:
        print(f"❌ Kafka is NOT running at {settings.KAFKA_BOOTSTRAP_SERVERS}")
        print("\n💡 Start Kafka with: make start")
        return False


def check_redis():
    """Check if Redis is accessible."""
    print_section("2. Checking Redis Connection")
    try:
        r = redis.from_url(settings.REDIS_URL, socket_timeout=2)
        r.ping()
        print(f"✅ Redis is running at {settings.REDIS_URL}")
        return True
    except (redis.ConnectionError, redis.TimeoutError):
        print(f"❌ Redis is NOT running at {settings.REDIS_URL}")
        print("\n💡 Start Redis with: make start")
        return False


def check_postgres():
    """Check if Postgres is accessible."""
    print_section("3. Checking PostgreSQL Connection")
    try:
        engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        print(f"✅ PostgreSQL is running")

        # Check if table exists
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'stg'
                    AND table_name = 'complete_trip'
                )
            """
                )
            )
            table_exists = result.scalar()

        if table_exists:
            print(f"✅ Table stg.complete_trip exists")
        else:
            print(f"⚠️  Table stg.complete_trip does NOT exist")
            print("\n💡 Run migrations with: make db-migrate")
            return False

        return True
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        print("\n💡 Start PostgreSQL with: make start")
        return False


def send_sample_events():
    """Send sample trip events to Kafka."""
    print_section("4. Sending Sample Events to Kafka")

    topic = "trips.normalized"
    trip_key = f"demo_trip_{int(time.time())}"

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    # Define events in temporal order
    events = [
        {
            "event_type": "IDENTITY",
            "event_version": "1",
            "event_ts": "2025-10-21T10:00:00",
            "trip_key": trip_key,
            "payload": {
                "vendor_id": 2,
                "vehicle_id_h": "demo_vehicle_001",
                "pickup_ts": "2025-10-21T10:00:00",
                "pu_zone_id": 161,
                "rate_code": 1,
                "store_and_fwd_flag": "N",
            },
        },
        {
            "event_type": "PASSENGERS",
            "event_version": "1",
            "event_ts": "2025-10-21T10:00:10",
            "trip_key": trip_key,
            "payload": {"passenger_count": 2},
        },
        {
            "event_type": "FARE",
            "event_version": "1",
            "event_ts": "2025-10-21T10:15:00",
            "trip_key": trip_key,
            "payload": {
                "fare_amount": 18.50,
                "total_amount": 24.80,
                "payment_type": 1,
                "tolls_amount": 2.00,
                "congestion_surcharge": 2.50,
                "airport_fee": 0.00,
            },
        },
        {
            "event_type": "DROP",
            "event_version": "1",
            "event_ts": "2025-10-21T10:30:00",
            "trip_key": trip_key,
            "payload": {
                "dropoff_ts": "2025-10-21T10:30:00",
                "do_zone_id": 237,
                "distance_km": 12.87,
                "duration_min": 22.0,
                "avg_speed_kmh": 35.1,
            },
        },
    ]

    print(f"Trip Key: {trip_key}")
    print(f"Sending {len(events)} events to topic '{topic}'...\n")

    for i, event in enumerate(events, 1):
        producer.send(topic, key=trip_key, value=event)
        print(f"  {i}. Sent {event['event_type']} event")
        time.sleep(0.1)  # Small delay between events

    producer.flush()
    producer.close()

    print(f"\n✅ Sent {len(events)} events successfully!")
    print(f"\n💡 These events are now in Kafka waiting for the assembler to consume them.")

    return trip_key


def show_redis_state(trip_key: str):
    """Show the current state in Redis."""
    print_section("5. Checking Redis State")

    r = redis.from_url(settings.REDIS_URL)
    redis_key = f"trip:{trip_key}"

    data = r.get(redis_key)
    if data:
        partial_trip = json.loads(data)
        print(f"🔍 Partial trip found in Redis:")
        print(f"   Events received: {partial_trip.get('events_received', [])}")
        print(f"   Pickup time: {partial_trip.get('pickup_ts')}")
        print(f"   Dropoff time: {partial_trip.get('dropoff_ts', 'NOT YET')}")
        print(f"   Last update: {partial_trip.get('last_event_ts')}")
        return partial_trip
    else:
        print(f"❌ No partial trip found in Redis for key: {redis_key}")
        print(f"   (This means either the assembler hasn't processed it yet,")
        print(f"    or it was already completed and written to PostgreSQL)")
        return None


def show_postgres_results(trip_key: str):
    """Show trips in PostgreSQL."""
    print_section("6. Checking PostgreSQL Results")

    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Check for our specific trip
        result = conn.execute(
            text("SELECT * FROM stg.complete_trip WHERE trip_key = :trip_key"),
            {"trip_key": trip_key},
        )
        row = result.fetchone()

        if row:
            print(f"✅ Trip found in database!")
            print(f"\n   Trip Key: {row.trip_key}")
            print(f"   Vendor: {row.vendor_id}")
            print(f"   Vehicle: {row.vehicle_id_h}")
            print(f"   Pickup: {row.pickup_ts}")
            print(f"   Dropoff: {row.dropoff_ts}")
            print(f"   Passengers: {row.passenger_count}")
            print(f"   Distance: {row.distance_km} km")
            print(f"   Duration: {row.duration_min} min")
            print(f"   Fare: ${row.fare_amount}")
            print(f"   Total: ${row.total_amount}")
            print(f"   Source: {row.source}")
        else:
            print(f"❌ Trip not found in database yet.")
            print(f"   Key: {trip_key}")
            print(f"\n   💡 The assembler may not have processed it yet.")

        # Show total count
        result = conn.execute(text("SELECT COUNT(*) FROM stg.complete_trip"))
        count = result.scalar()
        print(f"\n📊 Total trips in database: {count}")


def run_assembler_manually():
    """Show how to run the assembler manually."""
    print_section("7. Running the Assembler")

    print("To consume and process the events, run the assembler in another terminal:\n")
    print("  Option 1 (via Makefile):")
    print("    make assembler\n")
    print("  Option 2 (directly):")
    print("    cd packages/stream-trip-assembler")
    print("    python -m fluxframe.cli --verbose\n")
    print("  Option 3 (with custom settings):")
    print("    cd packages/stream-trip-assembler")
    print("    python -m fluxframe.cli --watermark 60 --verbose\n")
    print("The assembler will:")
    print("  1. Connect to Kafka and start consuming from trips.normalized")
    print("  2. Store partial trips in Redis")
    print("  3. Write complete trips to PostgreSQL")
    print("  4. Handle late-arriving TIP events")
    print("\nPress Ctrl+C to stop the assembler when done.")


def manual_event_sending_guide():
    """Show how to send events manually for testing."""
    print_section("8. Manual Event Testing")

    print("You can also send events manually using:")
    print("\n1️⃣  Python script (like this demo):")
    print("    python scripts/demo/demo_trip_assembler.py\n")

    print("2️⃣  Redpanda command-line tools (rpk):")
    print("    docker exec -it fluxframe-redpanda rpk topic produce trips.normalized \\")
    print("      --key demo_trip_123 \\")
    print("      --partition 0\n")
    print("    Then paste JSON events (one per line):\n")
    print('    {"event_type":"IDENTITY","event_version":"1",...}\n')
    print("    Or use Redpanda Console: http://localhost:8080\n")

    print("3️⃣  Via REST API (if we create an API endpoint):")
    print("    POST http://localhost:8000/api/events")
    print("    Body: {event JSON}\n")

    print("4️⃣  Via Replay Producer:")
    print("    make replay")
    print("    (This reads curated data and sends real events)\n")


def main():
    """Run the demo."""
    print("\n" + "=" * 70)
    print("  🚕 FluxFrame Trip Assembler Demo")
    print("=" * 70)

    # Check infrastructure
    kafka_ok = check_kafka()
    redis_ok = check_redis()
    postgres_ok = check_postgres()

    if not (kafka_ok and redis_ok and postgres_ok):
        print("\n❌ Infrastructure not ready. Please start services first:")
        print("   make start")
        sys.exit(1)

    # Send sample events
    trip_key = send_sample_events()

    # Show how to run assembler
    run_assembler_manually()

    # Wait a bit for user to start assembler
    print(f"\n⏳ Waiting 10 seconds for you to start the assembler...")
    print(f"   (Or press Ctrl+C to skip)")
    try:
        time.sleep(10)
    except KeyboardInterrupt:
        print("\n⏩ Skipped waiting")

    # Check results
    show_redis_state(trip_key)
    show_postgres_results(trip_key)

    # Show manual testing guide
    manual_event_sending_guide()

    print_section("✨ Demo Complete!")
    print("Next steps:")
    print("  1. Run 'make assembler' to start consuming events")
    print("  2. Run 'make replay' to send real trip data")
    print("  3. Check PostgreSQL: make db-shell")
    print("     SELECT COUNT(*) FROM stg.complete_trip;")
    print()


if __name__ == "__main__":
    main()
