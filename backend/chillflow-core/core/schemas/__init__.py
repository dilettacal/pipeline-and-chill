"""Pydantic schemas for ChillFlow data validation and serialization."""

from core.schemas.events import (
    BaseEvent,
    DropEvent,
    EventType,
    FareEvent,
    IdentityEvent,
    PassengersEvent,
    TipEvent,
    TripEventUnion,
    deserialize_event,
)
from core.schemas.trip import CompleteTripSchema, TripCreateSchema, TripUpdateSchema, ZoneSchema

__all__ = [
    # Trip schemas
    "CompleteTripSchema",
    "TripCreateSchema",
    "TripUpdateSchema",
    "ZoneSchema",
    # Event schemas
    "BaseEvent",
    "IdentityEvent",
    "PassengersEvent",
    "FareEvent",
    "DropEvent",
    "TipEvent",
    "TripEventUnion",
    "EventType",
    "deserialize_event",
]
