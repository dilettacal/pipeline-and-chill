"""
Shared Pydantic schemas and type aliases for the ChillFlow system.

This module provides common type aliases and utilities for consistent
data validation and serialization across the ChillFlow system.
"""

from datetime import datetime
from typing import Annotated

from pydantic.functional_serializers import PlainSerializer

# Type alias for datetime fields that serialize to ISO 8601 format
IsoDatetime = Annotated[
    datetime,
    PlainSerializer(lambda v: v.isoformat(), return_type=str, when_used="json"),
]

# Import all schemas from their respective modules
from .events import (
    BaseEvent,
    DropEvent,
    EventType,
    FareEvent,
    IdentityEvent,
    PassengersEvent,
    TipEvent,
    deserialize_event,
)
from .trip import CompleteTripSchema, TripCreateSchema, TripUpdateSchema, ZoneSchema

__all__ = [
    "IsoDatetime",
    "BaseEvent",
    "DropEvent",
    "EventType",
    "FareEvent",
    "IdentityEvent",
    "PassengersEvent",
    "TipEvent",
    "deserialize_event",
    "CompleteTripSchema",
    "TripCreateSchema",
    "TripUpdateSchema",
    "ZoneSchema",
]
