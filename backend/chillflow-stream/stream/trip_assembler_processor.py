"""
Trip Assembler Processor - Pure business logic for trip assembly.

This module contains the core business logic for processing trip events
and assembling complete trips, without any IO concerns.
"""

from typing import Any, Dict, Optional

from core import CompleteTrip
from core.utils.logging import get_logger

from .completion_policy import CompletionPolicy
from .events import (
    EventType,
    PaymentProcessedEvent,
    TripEndedEvent,
    TripEvent,
    TripStartedEvent,
    ZoneEnteredEvent,
    ZoneExitedEvent,
)
from .trip_builder import TripBuilder


class TripAssemblerProcessor:
    """
    Stateless processor that handles trip event processing and assembly.

    This class contains the pure business logic for:
    - Parsing incoming events
    - Updating state atomically
    - Building complete trips when ready
    """

    def __init__(self, state_manager, builder: TripBuilder, policy: CompletionPolicy):
        """
        Initialize the processor.

        Args:
            state_manager: State manager for atomic operations
            builder: Trip builder for creating CompleteTrip objects
            policy: Completion policy for determining trip completeness
        """
        self.state_manager = state_manager
        self.builder = builder
        self.policy = policy
        self.log = get_logger("chillflow-stream.trip-assembler-processor")

    def process(self, raw_event: Dict[str, Any]) -> Optional[CompleteTrip]:
        """
        Process a raw event and return a complete trip if ready.

        Args:
            raw_event: Raw event data from Kafka

        Returns:
            CompleteTrip if trip is complete, None otherwise
        """
        # Parse the event
        event = self._parse_event(raw_event)
        if not event:
            self.log.warning("Unknown/invalid event", raw=raw_event)
            return None

        trip_key = event.trip_key

        # Update state atomically and check completion
        is_complete, snapshot = self.state_manager.upsert_event_and_check_complete(trip_key, event)

        if not is_complete:
            self.log.debug("Trip not complete yet", trip_key=trip_key)
            return None

        # Build complete trip
        try:
            trip = self.builder.build(trip_key, snapshot)
            self.log.info("Trip completed and built", trip_key=trip_key)
            return trip
        except Exception as e:
            self.log.error("Failed to build complete trip", trip_key=trip_key, error=str(e))
            return None

    def _parse_event(self, data: Dict[str, Any]) -> Optional[TripEvent]:
        """
        Parse raw event data into a typed TripEvent.

        Args:
            data: Raw event data

        Returns:
            Parsed TripEvent or None if invalid
        """
        try:
            event_type = data.get("event_type")

            if event_type == EventType.TRIP_STARTED.value:
                return TripStartedEvent(**data)
            elif event_type == EventType.TRIP_ENDED.value:
                return TripEndedEvent(**data)
            elif event_type == EventType.PAYMENT_PROCESSED.value:
                return PaymentProcessedEvent(**data)
            elif event_type == EventType.ZONE_ENTERED.value:
                return ZoneEnteredEvent(**data)
            elif event_type == EventType.ZONE_EXITED.value:
                return ZoneExitedEvent(**data)
            else:
                self.log.warning("Unknown event type", event_type=event_type)
                return None

        except Exception as e:
            self.log.error("Failed to parse event", error=str(e), data=data)
            return None
