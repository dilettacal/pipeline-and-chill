"""
State management for trip assembly.

Provides abstract interface for both in-memory and Redis-based state management.
"""

from abc import ABC, abstractmethod
from typing import Dict, Tuple

from .events import TripEvent


class StateManager(ABC):
    """Abstract state manager for trip assembly."""

    @abstractmethod
    def store_event(self, trip_key: str, event: TripEvent) -> None:
        """Store an event for a trip."""

    @abstractmethod
    def get_trip_events(self, trip_key: str) -> Dict[str, TripEvent]:
        """Get all events for a trip."""

    @abstractmethod
    def delete_trip(self, trip_key: str) -> bool:
        """Delete a trip and all its events."""

    @abstractmethod
    def get_partial_trip_count(self) -> int:
        """Get the number of partial trips."""

    @abstractmethod
    def upsert_event_and_check_complete(self, trip_key: str, event: TripEvent) -> Tuple[bool, Dict]:
        """
        Atomically upsert an event and check if trip is complete.

        Args:
            trip_key: The trip identifier
            event: The event to upsert

        Returns:
            Tuple of (is_complete, snapshot) where snapshot contains normalized data
        """


class InMemoryStateManager(StateManager):
    """In-memory state manager (current implementation)."""

    def __init__(self):
        """Initialize in-memory state manager."""
        self.partial_trips: Dict[str, Dict[str, TripEvent]] = {}

    def store_event(self, trip_key: str, event: TripEvent) -> None:
        """Store an event for a trip."""
        if trip_key not in self.partial_trips:
            self.partial_trips[trip_key] = {}

        self.partial_trips[trip_key][event.event_type.value] = event

    def get_trip_events(self, trip_key: str) -> Dict[str, TripEvent]:
        """Get all events for a trip."""
        return self.partial_trips.get(trip_key, {})

    def delete_trip(self, trip_key: str) -> bool:
        """Delete a trip and all its events."""
        if trip_key in self.partial_trips:
            del self.partial_trips[trip_key]
            return True
        return False

    def get_partial_trip_count(self) -> int:
        """Get the number of partial trips."""
        return len(self.partial_trips)

    def upsert_event_and_check_complete(self, trip_key: str, event: TripEvent) -> Tuple[bool, Dict]:
        """Atomically upsert event and check completion."""
        # Store the event
        self.store_event(trip_key, event)

        # Get all events for this trip
        trip_events = self.get_trip_events(trip_key)

        # Check if trip is complete (simplified for now)
        # TODO: Use CompletionPolicy for proper checking
        required_events = {"trip_started", "trip_ended", "payment_processed"}
        present_events = set(trip_events.keys())

        is_complete = required_events.issubset(present_events)

        if is_complete:
            # Build snapshot from events
            snapshot = self._build_snapshot(trip_events)
            return True, snapshot
        else:
            return False, {}

    def _build_snapshot(self, trip_events: Dict[str, TripEvent]) -> Dict:
        """Build normalized snapshot from trip events."""
        snapshot = {}

        for event_type, event in trip_events.items():
            # Add common fields
            if hasattr(event, "vendor_id"):
                snapshot["vendor_id"] = str(event.vendor_id)
            if hasattr(event, "fare_amount"):
                snapshot["fare_amount"] = str(event.fare_amount)
            if hasattr(event, "passenger_count"):
                snapshot["passenger_count"] = str(event.passenger_count)
            if hasattr(event, "tip_amount"):
                snapshot["tip_amount"] = str(event.tip_amount)
            if hasattr(event, "total_amount"):
                snapshot["total_amount"] = str(event.total_amount)

            # Add timestamp fields
            snapshot[f"{event_type}_timestamp"] = event.timestamp.isoformat()

        return snapshot


class RedisStateManager(StateManager):
    """Redis-based state manager for production use."""

    def __init__(self, redis_url: str, ttl_seconds: int = 21600):  # 6 hours
        """
        Initialize Redis state manager.

        Args:
            redis_url: Redis connection URL
            ttl_seconds: TTL for partial trips in seconds
        """
        from core.clients.redis import RedisClient

        self.redis_client = RedisClient(redis_url=redis_url)
        self.ttl_seconds = ttl_seconds
        self.key_prefix = "trip:"

    def _make_key(self, trip_key: str) -> str:
        """Generate Redis key for a trip."""
        return f"{self.key_prefix}{trip_key}"

    def store_event(self, trip_key: str, event: TripEvent) -> None:
        """Store an event for a trip."""
        redis_key = self._make_key(trip_key)

        from core.utils.logging import get_logger

        logger = get_logger("redis-state-manager")
        logger.info(
            "Storing event in Redis",
            trip_key=trip_key,
            event_type=event.event_type.value,
            redis_key=redis_key,
        )

        # Get existing events
        existing_events = self.get_trip_events(trip_key)
        existing_events[event.event_type.value] = event

        # Store updated events with TTL
        import json

        events_data = {
            event_type: event.model_dump() for event_type, event in existing_events.items()
        }

        result = self.redis_client.set(
            redis_key, json.dumps(events_data, default=str), ex=self.ttl_seconds
        )
        logger.info(
            "Event stored in Redis", trip_key=trip_key, success=result, ttl=self.ttl_seconds
        )

    def get_trip_events(self, trip_key: str) -> Dict[str, TripEvent]:
        """Get all events for a trip."""
        redis_key = self._make_key(trip_key)
        data = self.redis_client.get(redis_key)

        if not data:
            return {}

        import json

        from .events import (
            PaymentProcessedEvent,
            TripEndedEvent,
            TripStartedEvent,
            ZoneEnteredEvent,
            ZoneExitedEvent,
        )

        events_data = json.loads(data)
        events = {}

        for event_type, event_dict in events_data.items():
            if event_type == "trip_started":
                events[event_type] = TripStartedEvent(**event_dict)
            elif event_type == "trip_ended":
                events[event_type] = TripEndedEvent(**event_dict)
            elif event_type == "payment_processed":
                events[event_type] = PaymentProcessedEvent(**event_dict)
            elif event_type == "zone_entered":
                events[event_type] = ZoneEnteredEvent(**event_dict)
            elif event_type == "zone_exited":
                events[event_type] = ZoneExitedEvent(**event_dict)

        return events

    def delete_trip(self, trip_key: str) -> bool:
        """Delete a trip and all its events."""
        redis_key = self._make_key(trip_key)
        deleted = self.redis_client.delete(redis_key)
        return bool(deleted)

    def get_partial_trip_count(self) -> int:
        """Get the number of partial trips."""
        # Simple implementation - use keys command (not efficient for production)
        # In production, you'd want to use Redis Streams or a different approach
        try:
            # Get all keys matching our pattern
            keys = self.redis_client.client.keys(f"{self.key_prefix}*")
            return len(keys)
        except Exception:
            # Fallback if keys command is not available
            return 0

    def upsert_event_and_check_complete(self, trip_key: str, event: TripEvent) -> Tuple[bool, Dict]:
        """Atomically upsert event and check completion."""
        # Store the event using existing method
        self.store_event(trip_key, event)

        # Get all events for this trip
        trip_events = self.get_trip_events(trip_key)

        # Check if trip is complete (simplified for now)
        # TODO: Use CompletionPolicy for proper checking
        required_events = {"trip_started", "trip_ended", "payment_processed"}
        present_events = set(trip_events.keys())

        is_complete = required_events.issubset(present_events)

        if is_complete:
            # Build snapshot from events
            snapshot = self._build_snapshot(trip_events)
            return True, snapshot
        else:
            return False, {}

    def _build_snapshot(self, trip_events: Dict[str, TripEvent]) -> Dict:
        """Build normalized snapshot from trip events."""
        snapshot = {}

        for event_type, event in trip_events.items():
            # Add common fields
            if hasattr(event, "vendor_id"):
                snapshot["vendor_id"] = str(event.vendor_id)
            if hasattr(event, "fare_amount"):
                snapshot["fare_amount"] = str(event.fare_amount)
            if hasattr(event, "passenger_count"):
                snapshot["passenger_count"] = str(event.passenger_count)
            if hasattr(event, "tip_amount"):
                snapshot["tip_amount"] = str(event.tip_amount)
            if hasattr(event, "total_amount"):
                snapshot["total_amount"] = str(event.total_amount)

            # Add timestamp fields
            snapshot[f"{event_type}_timestamp"] = event.timestamp.isoformat()

        return snapshot
