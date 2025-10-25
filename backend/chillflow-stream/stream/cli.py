"""
ChillFlow Stream CLI - Command-line interface for streaming operations.

This module provides CLI commands for the stream service, including
trip event production and real-time processing.
"""

import sys
from typing import Optional

import click
import pandas as pd
from core import get_logger as get_core_logger
from core.clients.database import get_db_session
from sqlalchemy import text

from .trip_assembler import TripAssembler
from .trip_event_producer import TripEventProducer

logger = get_core_logger("chillflow-stream.cli")


@click.group()
@click.option("--log-level", default="INFO", help="Logging level")
def main(log_level: str):
    """ChillFlow Stream CLI - Real-time trip event processing."""
    logger.info("ChillFlow Stream CLI started", log_level=log_level)


@main.command()
@click.option("--trip-key", help="Specific trip key to process")
@click.option("--limit", type=int, default=100, help="Number of trips to process")
@click.option("--topic", default="trip-events", help="Kafka topic name")
@click.option("--batch-size", type=int, default=50, help="Batch size for processing")
def produce_events(trip_key: Optional[str], limit: int, topic: str, batch_size: int):
    """Produce events from complete trips in the database."""
    logger.info("Starting trip event production", trip_key=trip_key, limit=limit, topic=topic)

    try:
        # Get trips from database
        session_gen = get_db_session()
        session = next(session_gen)

        try:
            if trip_key:
                # Process specific trip
                query = text("SELECT * FROM stg.complete_trip WHERE trip_key = :trip_key")
                result = session.execute(query, {"trip_key": trip_key}).fetchone()
                if not result:
                    logger.error("Trip not found", trip_key=trip_key)
                    return
                trips_df = pd.DataFrame([result._asdict()])
            else:
                # Process multiple trips
                query = text("SELECT * FROM stg.complete_trip ORDER BY pickup_ts LIMIT :limit")
                result = session.execute(query, {"limit": limit}).fetchall()
                if not result:
                    logger.error("No trips found in database")
                    return
                trips_df = pd.DataFrame([row._asdict() for row in result])

            logger.info("Found trips for processing", count=len(trips_df))

            # Initialize producer
            producer = TripEventProducer()

            try:
                # Process trips and send events
                stats = producer.process_trips_from_dataframe(trips_df, topic, batch_size)

                logger.info(
                    "Event production complete",
                    total_trips=stats["total_trips"],
                    total_events=stats["total_events"],
                    sent=stats["sent"],
                    failed=stats["failed"],
                )

                click.echo(f"✅ Processed {stats['total_trips']} trips")
                click.echo(f"📡 Generated {stats['total_events']} events")
                click.echo(f"📤 Sent {stats['sent']} events to topic '{topic}'")
                if stats["failed"] > 0:
                    click.echo(f"❌ Failed to send {stats['failed']} events")

            finally:
                producer.close()

        finally:
            session.close()

    except Exception as e:
        logger.error("Event production failed", error=str(e))
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option("--file", type=click.Path(exists=True), help="Parquet file with trip data")
@click.option("--topic", default="trip-events", help="Kafka topic name")
@click.option("--batch-size", type=int, default=50, help="Batch size for processing")
def produce_from_file(file: str, topic: str, batch_size: int):
    """Produce events from a parquet file with trip data."""
    logger.info("Starting trip event production from file", file=file, topic=topic)

    try:
        # Read parquet file
        df = pd.read_parquet(file)
        logger.info("Loaded trip data from file", rows=len(df))

        # Initialize producer
        producer = TripEventProducer()

        try:
            # Process trips and send events
            stats = producer.process_trips_from_dataframe(df, topic, batch_size)

            logger.info(
                "Event production complete",
                total_trips=stats["total_trips"],
                total_events=stats["total_events"],
                sent=stats["sent"],
                failed=stats["failed"],
            )

            click.echo(f"✅ Processed {stats['total_trips']} trips from file")
            click.echo(f"📡 Generated {stats['total_events']} events")
            click.echo(f"📤 Sent {stats['sent']} events to topic '{topic}'")
            if stats["failed"] > 0:
                click.echo(f"❌ Failed to send {stats['failed']} events")

        finally:
            producer.close()

    except Exception as e:
        logger.error("Event production from file failed", error=str(e))
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option("--topic", default="trip-events", help="Kafka topic name")
@click.option("--timeout", type=int, default=30, help="Consumer timeout in seconds")
def consume_events(topic: str, timeout: int):
    """Consume and display events from Kafka topic."""
    logger.info("Starting event consumption", topic=topic, timeout=timeout)

    try:
        import json

        # Initialize consumer
        import os

        from kafka import KafkaConsumer

        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout * 1000,
        )

        try:
            event_count = 0
            click.echo(f"🔍 Consuming events from topic '{topic}'...")

            for message in consumer:
                event = message.value
                event_count += 1

                click.echo(f"📡 Event {event_count}:")
                click.echo(f"   Trip: {event['trip_key']}")
                click.echo(f"   Type: {event['event_type']}")
                click.echo(f"   Time: {event['timestamp']}")
                click.echo(f"   Vendor: {event['vendor_id']}")
                click.echo("")

            if event_count == 0:
                click.echo(f"⚠️  No events found in topic '{topic}'")
            else:
                click.echo(f"✅ Consumed {event_count} events")

        finally:
            consumer.close()

    except Exception as e:
        logger.error("Event consumption failed", error=str(e))
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option("--topic", default="trip-events", help="Kafka topic name")
@click.option("--timeout", type=int, default=30, help="Consumer timeout in seconds")
@click.option("--max-events", type=int, help="Maximum number of events to process")
@click.option("--save-db/--no-save-db", default=True, help="Save completed trips to database")
@click.option("--use-redis/--no-redis", default=False, help="Use Redis for state management")
@click.option("--redis-url", default="redis://localhost:6379/0", help="Redis connection URL")
def assemble_trips(
    topic: str,
    timeout: int,
    max_events: Optional[int],
    save_db: bool,
    use_redis: bool,
    redis_url: str,
):
    """Assemble events into complete trips."""
    logger.info(
        "Starting trip assembly", topic=topic, timeout=timeout, save_db=save_db, use_redis=use_redis
    )

    try:
        # Initialize assembler with Redis support
        assembler = TripAssembler(topic=topic, use_redis=use_redis, redis_url=redis_url)

        try:
            # Run assembly loop
            stats = assembler.run_assembly_loop(
                timeout_ms=timeout * 1000,
                max_events=max_events,
                save_to_db=save_db,
            )

            logger.info(
                "Trip assembly complete",
                trips_assembled=stats["trips_assembled"],
                trips_saved=stats["trips_saved"],
                trips_failed=stats["trips_failed"],
            )

            click.echo(f"✅ Assembled {stats['trips_assembled']} trips")
            if use_redis:
                click.echo(f"🔴 Using Redis state management")
            else:
                click.echo(f"💾 Using in-memory state management")
            if save_db:
                click.echo(f"💾 Saved {stats['trips_saved']} trips to database")
                if stats["trips_failed"] > 0:
                    click.echo(f"❌ Failed to save {stats['trips_failed']} trips")

        finally:
            assembler.close()

    except Exception as e:
        logger.error("Trip assembly failed", error=str(e))
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option("--topic", default="trip-events", help="Kafka topic name")
def create_topic(topic: str):
    """Create Kafka topic for trip events."""
    logger.info("Creating Kafka topic", topic=topic)

    try:
        from kafka.admin import KafkaAdminClient, NewTopic

        # Initialize admin client
        admin_client = KafkaAdminClient(bootstrap_servers=["localhost:9092"])

        try:
            # Create topic
            topic_obj = NewTopic(
                name=topic,
                num_partitions=3,
                replication_factor=1,
            )

            admin_client.create_topics([topic_obj])
            logger.info("Topic created successfully", topic=topic)
            click.echo(f"✅ Created topic '{topic}' with 3 partitions")

        finally:
            admin_client.close()

    except Exception as e:
        logger.error("Topic creation failed", error=str(e))
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option("--topic", default="trip-events", help="Kafka topic name")
@click.option("--max-events", type=int, help="Maximum number of events to process")
@click.option("--redis-url", default="redis://localhost:6379/0", help="Redis connection URL")
def ingest_events(
    topic: str,
    max_events: Optional[int],
    redis_url: str,
):
    """Ingest events from Kafka to Redis (Expert Architecture)."""
    logger.info("Starting event ingestion", topic=topic, max_events=max_events, redis_url=redis_url)

    try:
        from .event_ingestor import EventIngestor

        # Initialize event ingestor
        ingestor = EventIngestor(topic=topic, redis_url=redis_url)

        click.echo(f"📡 Ingesting events from {topic} to Redis...")

        # Process events
        events_processed = ingestor.consume_events(max_events=max_events)

        # Show results
        click.echo(f"✅ Processed {events_processed} events")
        click.echo(f"📊 Events stored in Redis")

    except Exception as e:
        logger.error("Event ingestion failed", error=str(e))
        click.echo(f"❌ Error: {e}")
        raise click.Abort()


if __name__ == "__main__":
    main()
