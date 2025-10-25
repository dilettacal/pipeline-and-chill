#!/usr/bin/env python3
"""
Demo script for Replay Producer.

This script demonstrates the replay producer functionality by:
1. Checking prerequisites (Kafka running, curated data)
2. Running a small sample replay (1000 trips)
3. Showing events produced to Kafka
4. Displaying statistics and verification
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Add package paths
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "packages" / "core-utils"))
sys.path.insert(0, str(project_root / "packages" / "ingestion-replay-producer"))

from fluxframe.events import decompose_trip
from fluxframe.hashing import generate_trip_key, generate_vehicle_id_h
from fluxframe.producer import TripReplayProducer
from fluxframe.settings import settings


def print_header(text: str):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def print_section(text: str):
    """Print a formatted section header."""
    print(f"\n{'─' * 80}")
    print(f"  {text}")
    print("─" * 80)


def check_kafka() -> bool:
    """Check if Kafka is running and accessible."""
    print_section("1. Checking Kafka Connection")

    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=5000
        )
        producer.close()
        print(f"✅ Kafka is running at {settings.KAFKA_BOOTSTRAP_SERVERS}")
        return True
    except NoBrokersAvailable:
        print(f"❌ Kafka not accessible at {settings.KAFKA_BOOTSTRAP_SERVERS}")
        print("\n💡 Start Kafka with:")
        print("   make start")
        print("   # OR")
        print("   docker compose -f infra/docker-compose.yml up -d")
        return False
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        return False


def check_curated_data() -> tuple[bool, Path | None]:
    """Check if curated data exists."""
    print_section("2. Checking Curated Data")

    data_path = Path("data/curated/yellow/2025/05/trips_clean.parquet")

    if not data_path.exists():
        print(f"❌ Curated data not found at {data_path}")
        print("\n💡 Run data curation first:")
        print("   cd packages/ingestion-monthly-loader")
        print("   python -m fluxframe.cli --month 2025-05")
        return False, None

    # Check file size
    size_mb = data_path.stat().st_size / (1024 * 1024)
    print(f"✅ Curated data found: {data_path}")
    print(f"   Size: {size_mb:.1f} MB")

    # Quick peek at data
    df = pd.read_parquet(data_path)
    print(f"   Rows: {len(df):,} trips")
    print(f"   Columns: {len(df.columns)}")

    return True, data_path


def check_topic_exists() -> bool:
    """Check if Kafka topic exists."""
    print_section("3. Checking Kafka Topic")

    try:
        result = subprocess.run(
            ["docker", "exec", "fluxframe-redpanda", "rpk", "topic", "list"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if "trips.normalized" in result.stdout:
            print("✅ Topic 'trips.normalized' exists")

            # Get topic details
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "fluxframe-redpanda",
                    "rpk",
                    "topic",
                    "describe",
                    "trips.normalized",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            print(f"\n{result.stdout}")
            return True
        else:
            print("⚠️  Topic 'trips.normalized' not found")
            print("\n💡 Create topic with:")
            print("   ./scripts/infrastructure/setup_kafka_topics.sh")
            return False

    except subprocess.TimeoutExpired:
        print("⚠️  Timeout checking topic")
        return False
    except FileNotFoundError:
        print("⚠️  Docker not available")
        return False
    except Exception as e:
        print(f"⚠️  Error checking topic: {e}")
        return False


def demonstrate_event_decomposition():
    """Show how a trip is decomposed into events."""
    print_section("4. Event Decomposition Demo")

    # Load one sample trip
    data_path = Path("data/curated/yellow/2025/05/trips_clean.parquet")
    df = pd.read_parquet(data_path)
    row = df.iloc[0].copy()  # Make a copy to avoid SettingWithCopyWarning

    print("Sample trip:")
    print(f"  Pickup:  {row['pickup_ts']}")
    print(f"  Dropoff: {row['dropoff_ts']}")
    print(f"  Vendor:  {row['vendor_id']}")
    print(f"  Zone:    {row['pu_zone_id']} → {row['do_zone_id']}")
    print(f"  Fare:    ${row['fare_amount']:.2f}")
    print(f"  Tip:     ${row['tip_amount']:.2f}")

    # Generate trip key
    trip_key = generate_trip_key(
        salt=settings.HASH_SALT,
        vendor_id=int(row["vendor_id"]),
        pickup_ts=row["pickup_ts"],
        pu_zone_id=int(row["pu_zone_id"]),
        row_offset=0,
    )

    print(f"\nGenerated trip_key: {trip_key[:32]}...")

    # Generate vehicle_id_h (curated data doesn't have it yet)
    vehicle_id_h = generate_vehicle_id_h(
        salt=settings.HASH_SALT, vendor_id=int(row["vendor_id"]), row_offset=0
    )
    row["vehicle_id_h"] = vehicle_id_h
    print(f"Generated vehicle_id: {vehicle_id_h}")

    # Decompose into events
    events = decompose_trip(trip_key, row, late_arrival_pct=0.0)

    print(f"\n✨ Decomposed into {len(events)} events:\n")

    for i, event in enumerate(events, 1):
        event_dict = event.to_dict()
        print(f"{i}. {event.event_type:12} @ {event.event_ts.strftime('%H:%M:%S')}")
        print(f"   Payload keys: {list(event_dict['payload'].keys())}")

    print("\n📋 Full event example (IDENTITY):")
    print(json.dumps(events[0].to_dict(), indent=2, default=str)[:500] + "...")


def run_small_replay(data_path: Path, sample_size: int = 1000) -> int:
    """Run a small replay with limited trips."""
    print_section(f"5. Running Small Replay ({sample_size:,} trips)")

    # Read sample data
    print(f"\nReading {sample_size:,} trips from {data_path.name}...")
    df = pd.read_parquet(data_path)
    df = df.head(sample_size)  # Take only first N rows
    print(f"✅ Loaded {len(df):,} trips")

    # Initialize producer
    print(f"\nInitializing producer...")
    producer = TripReplayProducer(
        kafka_bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        topic="trips.normalized",
        replay_speed_multiplier=1000.0,  # Fast replay for demo
        late_arrival_pct=3.0,
    )

    # Generate events
    print(f"\nGenerating events...")
    start_time = time.time()
    events = producer.generate_events_for_month(df)
    gen_time = time.time() - start_time

    print(f"✅ Generated {len(events):,} events in {gen_time:.2f}s")
    print(f"   Average: {len(events) / len(df):.1f} events per trip")

    # Publish events
    print(f"\nPublishing to Kafka (fast mode)...")
    start_time = time.time()
    published = producer.replay_events(events, enable_sleep=False)
    publish_time = time.time() - start_time

    print(f"✅ Published {published:,} events in {publish_time:.2f}s")
    print(f"   Rate: {published / publish_time:.0f} events/sec")

    producer.close()

    return published


def verify_events_in_kafka(expected_count: int, sample_count: int = 5):
    """Verify events were published to Kafka."""
    print_section("6. Verifying Events in Kafka")

    try:
        print(f"Consuming first {sample_count} events from topic...")

        consumer = KafkaConsumer(
            "trips.normalized",
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        events_seen = []
        event_types = {}

        for message in consumer:
            event = message.value
            events_seen.append(event)
            event_type = event.get("event_type", "UNKNOWN")
            event_types[event_type] = event_types.get(event_type, 0) + 1

            if len(events_seen) >= sample_count:
                break

        consumer.close()

        if events_seen:
            print(f"✅ Successfully consumed {len(events_seen)} events\n")

            print("Sample events:")
            for i, event in enumerate(events_seen, 1):
                print(
                    f"\n{i}. {event.get('event_type', 'UNKNOWN')} @ {event.get('event_ts', 'N/A')}"
                )
                print(f"   trip_key: {event.get('trip_key', 'N/A')[:32]}...")
                print(f"   payload:  {list(event.get('payload', {}).keys())}")

            print(f"\n📊 Event type distribution (from sample):")
            for event_type, count in sorted(event_types.items()):
                print(f"   {event_type:12} : {count:3}")
        else:
            print("⚠️  No events consumed (topic might be empty)")

    except Exception as e:
        print(f"⚠️  Error consuming events: {e}")


def show_statistics():
    """Show summary statistics."""
    print_section("7. Summary Statistics")

    try:
        # Get topic info
        result = subprocess.run(
            [
                "docker",
                "exec",
                "fluxframe-redpanda",
                "rpk",
                "topic",
                "describe",
                "trips.normalized",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        print("Topic Statistics:")
        print(result.stdout)

    except Exception as e:
        print(f"Could not retrieve topic statistics: {e}")


def cleanup_demo_data():
    """Offer to clean up demo data."""
    print_section("8. Cleanup (Optional)")

    print("To clean up the demo events from Kafka:")
    print("  docker exec fluxframe-redpanda rpk topic delete trips.normalized")
    print("  ./scripts/infrastructure/setup_kafka_topics.sh  # Recreate topic")
    print("\nOr just let them expire (7-day retention)")


def main():
    """Run the demo."""
    print_header("🚀 FluxFrame Replay Producer Demo")

    print("This demo will:")
    print("  1. Check prerequisites (Kafka, curated data)")
    print("  2. Show event decomposition")
    print("  3. Run a small replay (1,000 trips → ~5,000 events)")
    print("  4. Verify events in Kafka")
    print("  5. Display statistics")

    # Check prerequisites
    kafka_ok = check_kafka()
    if not kafka_ok:
        print("\n❌ Cannot proceed without Kafka. Exiting.")
        sys.exit(1)

    data_ok, data_path = check_curated_data()
    if not data_ok:
        print("\n❌ Cannot proceed without curated data. Exiting.")
        sys.exit(1)

    topic_exists = check_topic_exists()
    if not topic_exists:
        print("\n⚠️  Proceeding anyway (topic will be auto-created)")

    # Demo event decomposition
    demonstrate_event_decomposition()

    # Wait for user
    print("\n" + "=" * 80)
    input("Press ENTER to run the small replay (1,000 trips)... ")

    # Run small replay
    published_count = run_small_replay(data_path, sample_size=1000)

    # Small delay to ensure messages are committed
    time.sleep(2)

    # Verify events
    verify_events_in_kafka(published_count, sample_count=10)

    # Statistics
    show_statistics()

    # Cleanup info
    cleanup_demo_data()

    # Final message
    print_header("✅ Demo Complete!")

    print("Next steps:")
    print("  • Replay full months: python -m fluxframe.cli --month 2025-05")
    print("  • Replay all streaming: python -m fluxframe.cli --all")
    print("  • Build Trip Assembler (Phase 5, Part 2)")
    print("  • Check Redpanda Console: http://localhost:8080")

    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
