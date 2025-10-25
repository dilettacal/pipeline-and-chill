"""
ChillFlow Stream Service - Real-time trip event processing.

This package provides streaming capabilities for the ChillFlow data pipeline,
including trip event production and real-time processing.
"""

from .events import (
    EventType,
    PaymentProcessedEvent,
    TripEndedEvent,
    TripEvent,
    TripStartedEvent,
    ZoneEnteredEvent,
    ZoneExitedEvent,
)
from .trip_assembler import TripAssembler
from .trip_event_producer import TripEventProducer

__all__ = [
    "EventType",
    "TripEvent",
    "TripStartedEvent",
    "TripEndedEvent",
    "PaymentProcessedEvent",
    "ZoneEnteredEvent",
    "ZoneExitedEvent",
    "TripEventProducer",
    "TripAssembler",
]
