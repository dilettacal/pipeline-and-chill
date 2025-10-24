"""
SQLAlchemy models for database tables.

This module contains shared database models used across multiple packages.
"""

from sqlalchemy import (
    CHAR,
    TIMESTAMP,
    Column,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CompleteTrip(Base):
    """
    SQLAlchemy model for stg.complete_trip table.

    Represents a complete taxi trip with all details. Can be populated
    either from streaming events (via trip-assembler) or batch processing
    (via batch-producer).
    """

    __tablename__ = "complete_trip"
    __table_args__ = {"schema": "stg"}

    # Primary key
    trip_key = Column(String(64), primary_key=True)

    # Identity fields
    vendor_id = Column(SmallInteger, nullable=False)
    vehicle_id_h = Column(String(64), nullable=False)
    pickup_ts = Column(TIMESTAMP, nullable=False)
    pu_zone_id = Column(Integer)
    rate_code = Column(SmallInteger)
    store_and_fwd_flag = Column(CHAR(1))

    # Passenger info
    passenger_count = Column(SmallInteger)

    # Fare info
    fare_amount = Column(Numeric(8, 2))
    total_amount = Column(Numeric(8, 2))
    payment_type = Column(SmallInteger)
    tolls_amount = Column(Numeric(8, 2))
    congestion_surcharge = Column(Numeric(8, 2))
    airport_fee = Column(Numeric(8, 2))

    # Drop info
    dropoff_ts = Column(TIMESTAMP, nullable=False)
    do_zone_id = Column(Integer)
    distance_km = Column(Numeric(8, 2))
    duration_min = Column(Numeric(8, 2))
    avg_speed_kmh = Column(Numeric(6, 2))

    # Tip info
    tip_amount = Column(Numeric(8, 2))

    # Metadata
    last_update_ts = Column(TIMESTAMP, nullable=False)
    source = Column(String(50), nullable=False)


# Indexes for query performance (created by Alembic migrations)
Index("idx_trip_pickup_ts", CompleteTrip.pickup_ts)
Index("idx_trip_dropoff_ts", CompleteTrip.dropoff_ts)
Index("idx_trip_pu_zone", CompleteTrip.pu_zone_id)
Index("idx_trip_do_zone", CompleteTrip.do_zone_id)
Index("idx_trip_vehicle", CompleteTrip.vehicle_id_h)
Index("idx_trip_source", CompleteTrip.source)
