"""ChillFlow Core Utilities - Shared utilities and settings."""

from chillflow.clients import (
    get_database_client,
    get_db_session,
    get_kafka_client,
    get_redis_client,
)

# Import data components
from chillflow.models import Base, CompleteTrip, Zone
from chillflow.schemas import (
    BaseEvent,
    CompleteTripSchema,
    DropEvent,
    EventType,
    FareEvent,
    IdentityEvent,
    PassengersEvent,
    TipEvent,
    TripCreateSchema,
    TripUpdateSchema,
    ZoneSchema,
    deserialize_event,
)
from chillflow.settings import settings
from chillflow.utils.hashing import generate_trip_key, generate_vehicle_id_h, sha256_hex
from chillflow.utils.logging import (
    get_logger,
    setup_console_logging,
    setup_development_logging,
    setup_json_file_logging,
)

__all__ = [
    # Core utilities
    "generate_trip_key",
    "generate_vehicle_id_h",
    "sha256_hex",
    "settings",
    "setup_console_logging",
    "setup_json_file_logging",
    "setup_development_logging",
    "get_logger",
    # Database models
    "Base",
    "CompleteTrip",
    "Zone",
    # Pydantic schemas
    "CompleteTripSchema",
    "TripCreateSchema",
    "TripUpdateSchema",
    "ZoneSchema",
    "BaseEvent",
    "IdentityEvent",
    "PassengersEvent",
    "FareEvent",
    "DropEvent",
    "TipEvent",
    "EventType",
    "deserialize_event",
    # Client utilities
    "get_database_client",
    "get_db_session",
    "get_redis_client",
    "get_kafka_client",
]
