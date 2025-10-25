"""
Trip Assembler - Reassembles streaming events into complete trips.

This module handles the consumption of individual trip events from Kafka
and reassembles them into complete trip records for database storage.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from core import CompleteTrip
from core import get_logger as get_core_logger
from core.clients.database import get_db_session
from kafka import KafkaConsumer
from structlog import get_logger

from .events import (
    EventType,
    PaymentProcessedEvent,
    TripEndedEvent,
    TripEvent,
    TripStartedEvent,
    ZoneEnteredEvent,
    ZoneExitedEvent,
)

logger = get_core_logger("chillflow-stream.trip-assembler")


class TripAssembler:
    """
    Assembles individual events into complete trip records.

    Consumes events from Kafka, groups them by trip_key, and creates
    complete trip records when all required events are received.
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        topic: str = "trip-events",
        group_id: str = "trip-assembler",
    ):
        """
        Initialize the trip assembler.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
            group_id: Kafka consumer group ID
        """
        self.topic = topic
        self.group_id = group_id
        self.kafka_consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

        # In-memory storage for partial trips
        self.partial_trips: Dict[str, Dict[str, TripEvent]] = {}
        self.completed_trips: List[CompleteTrip] = []

        logger.info(
            "Trip assembler initialized",
            servers=kafka_bootstrap_servers,
            topic=topic,
            group_id=group_id,
        )

    def process_event(self, event_data: dict) -> Optional[CompleteTrip]:
        """
        Process a single event and return complete trip if assembled.

        Args:
            event_data: Event data from Kafka

        Returns:
            CompleteTrip if trip is complete, None otherwise
        """
        try:
            # Parse event based on type
            event = self._parse_event(event_data)
            if not event:
                return None

            trip_key = event.trip_key

            # Initialize partial trip if not exists
            if trip_key not in self.partial_trips:
                self.partial_trips[trip_key] = {}

            # Store event
            self.partial_trips[trip_key][event.event_type.value] = event

            # Check if trip is complete
            complete_trip = self._check_trip_completion(trip_key)
            if complete_trip:
                # Remove from partial trips
                del self.partial_trips[trip_key]
                self.completed_trips.append(complete_trip)

                logger.info(
                    "Trip assembled successfully",
                    trip_key=trip_key,
                    vendor_id=complete_trip.vendor_id,
                )

                return complete_trip

            return None

        except Exception as e:
            logger.error(
                "Failed to process event",
                event_data=event_data,
                error=str(e),
            )
            return None

    def _parse_event(self, event_data: dict) -> Optional[TripEvent]:
        """Parse event data into appropriate event type."""
        event_type = event_data.get("event_type")

        if event_type == EventType.TRIP_STARTED.value:
            return TripStartedEvent(**event_data)
        elif event_type == EventType.TRIP_ENDED.value:
            return TripEndedEvent(**event_data)
        elif event_type == EventType.PAYMENT_PROCESSED.value:
            return PaymentProcessedEvent(**event_data)
        elif event_type == EventType.ZONE_ENTERED.value:
            return ZoneEnteredEvent(**event_data)
        elif event_type == EventType.ZONE_EXITED.value:
            return ZoneExitedEvent(**event_data)
        else:
            logger.warning("Unknown event type", event_type=event_type)
            return None

    def _check_trip_completion(self, trip_key: str) -> Optional[CompleteTrip]:
        """
        Check if a trip has all required events and assemble it.

        Args:
            trip_key: Trip identifier

        Returns:
            CompleteTrip if complete, None otherwise
        """
        events = self.partial_trips.get(trip_key, {})

        # Check for required events
        required_events = {
            EventType.TRIP_STARTED.value,
            EventType.TRIP_ENDED.value,
            EventType.PAYMENT_PROCESSED.value,
        }

        if not all(event_type in events for event_type in required_events):
            return None

        try:
            # Extract data from events
            trip_started = events[EventType.TRIP_STARTED.value]
            trip_ended = events[EventType.TRIP_ENDED.value]
            payment_processed = events[EventType.PAYMENT_PROCESSED.value]

            # Create complete trip
            complete_trip = CompleteTrip(
                trip_key=trip_key,
                vendor_id=trip_started.vendor_id,
                pickup_ts=trip_started.timestamp,
                dropoff_ts=trip_ended.timestamp,
                pu_zone_id=trip_started.pickup_zone_id,
                do_zone_id=trip_ended.dropoff_zone_id,
                passenger_count=trip_started.passenger_count,
                trip_distance=trip_ended.trip_distance,
                fare_amount=payment_processed.fare_amount,
                tip_amount=payment_processed.tip_amount,
                total_amount=payment_processed.total_amount,
                payment_type=payment_processed.payment_type,
                vehicle_id_h=trip_started.vehicle_id_h,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )

            return complete_trip

        except Exception as e:
            logger.error(
                "Failed to assemble trip",
                trip_key=trip_key,
                error=str(e),
            )
            return None

    def consume_events(
        self, timeout_ms: int = 1000, max_events: Optional[int] = None
    ) -> List[CompleteTrip]:
        """
        Consume events from Kafka and return completed trips.

        Args:
            timeout_ms: Consumer timeout in milliseconds
            max_events: Maximum number of events to process

        Returns:
            List of completed trips
        """
        completed_trips = []
        events_processed = 0

        logger.info("Starting event consumption", timeout_ms=timeout_ms, max_events=max_events)

        try:
            for message in self.kafka_consumer:
                # Process event
                complete_trip = self.process_event(message.value)
                if complete_trip:
                    completed_trips.append(complete_trip)

                events_processed += 1

                # Check limits
                if max_events and events_processed >= max_events:
                    break

                # Check timeout
                if timeout_ms and events_processed > 0:
                    # Simple timeout check - in production, use proper timeout handling
                    pass

        except Exception as e:
            logger.error("Event consumption failed", error=str(e))
            raise

        logger.info(
            "Event consumption complete",
            events_processed=events_processed,
            trips_completed=len(completed_trips),
        )

        return completed_trips

    def save_trips_to_database(self, trips: List[CompleteTrip]) -> Dict[str, int]:
        """
        Save completed trips to the database.

        Args:
            trips: List of completed trips

        Returns:
            Dict with save statistics
        """
        if not trips:
            return {"saved": 0, "failed": 0}

        session_gen = get_db_session()
        session = next(session_gen)

        try:
            saved_count = 0
            failed_count = 0

            for trip in trips:
                try:
                    # Use merge for upsert functionality
                    session.merge(trip)
                    saved_count += 1
                except Exception as e:
                    logger.error(
                        "Failed to save trip",
                        trip_key=trip.trip_key,
                        error=str(e),
                    )
                    failed_count += 1

            session.commit()

            logger.info(
                "Trips saved to database",
                total=len(trips),
                saved=saved_count,
                failed=failed_count,
            )

            return {"saved": saved_count, "failed": failed_count}

        except Exception as e:
            session.rollback()
            logger.error("Database save failed", error=str(e))
            raise
        finally:
            session.close()

    def run_assembly_loop(
        self,
        timeout_ms: int = 1000,
        max_events: Optional[int] = None,
        save_to_db: bool = True,
    ) -> Dict[str, int]:
        """
        Run the complete assembly loop: consume events and save trips.

        Args:
            timeout_ms: Consumer timeout in milliseconds
            max_events: Maximum number of events to process
            save_to_db: Whether to save completed trips to database

        Returns:
            Dict with processing statistics
        """
        logger.info("Starting trip assembly loop", save_to_db=save_to_db)

        # Consume events and assemble trips
        completed_trips = self.consume_events(timeout_ms, max_events)

        # Save to database if requested
        if save_to_db and completed_trips:
            save_stats = self.save_trips_to_database(completed_trips)
        else:
            save_stats = {"saved": 0, "failed": 0}

        logger.info(
            "Assembly loop complete",
            trips_assembled=len(completed_trips),
            trips_saved=save_stats["saved"],
            trips_failed=save_stats["failed"],
        )

        return {
            "trips_assembled": len(completed_trips),
            "trips_saved": save_stats["saved"],
            "trips_failed": save_stats["failed"],
        }

    def get_partial_trip_count(self) -> int:
        """Get the number of partial trips being assembled."""
        return len(self.partial_trips)

    def get_completed_trip_count(self) -> int:
        """Get the number of completed trips."""
        return len(self.completed_trips)

    def close(self):
        """Close Kafka consumer."""
        self.kafka_consumer.close()
        logger.info("Trip assembler closed")
