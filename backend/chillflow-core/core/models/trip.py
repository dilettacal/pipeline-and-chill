"""
SQLAlchemy models for trip-related data.

This module contains the database models for the ChillFlow trip processing system,
including complete trip records and related entities.
"""

from typing import Optional

from sqlalchemy import TIMESTAMP, Column, Float, ForeignKey, Integer, String, text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Zone(Base):
    """Taxi zone lookup table in dim schema."""

    __tablename__ = "zone"
    __table_args__ = {"schema": "dim"}

    zone_id = Column(Integer, primary_key=True, nullable=False)
    borough = Column(String(50), nullable=False)
    zone_name = Column(String(100), nullable=False)
    service_zone = Column(String(50), nullable=False)
    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )
    updated_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )

    # Relationships
    pickup_trips = relationship(
        "CompleteTrip",
        foreign_keys="CompleteTrip.pu_zone_id",
        back_populates="pickup_zone",
    )
    dropoff_trips = relationship(
        "CompleteTrip",
        foreign_keys="CompleteTrip.do_zone_id",
        back_populates="dropoff_zone",
    )


class CompleteTrip(Base):
    """Complete trip records in stg schema."""

    __tablename__ = "complete_trip"
    __table_args__ = {"schema": "stg"}

    # Primary key
    trip_key = Column(String(64), primary_key=True, nullable=False)

    # Trip metadata
    vendor_id = Column(Integer, nullable=False)
    pickup_ts = Column(TIMESTAMP, nullable=False)
    dropoff_ts = Column(TIMESTAMP, nullable=False)

    # Location data
    pu_zone_id = Column(
        Integer,
        ForeignKey("dim.zone.zone_id"),
        nullable=False,
    )
    do_zone_id = Column(
        Integer,
        ForeignKey("dim.zone.zone_id"),
        nullable=False,
    )

    # Trip details
    passenger_count = Column(Integer, nullable=True)
    trip_distance = Column(Float, nullable=True)
    fare_amount = Column(Float, nullable=True)
    tip_amount = Column(Float, nullable=True)
    total_amount = Column(Float, nullable=True)
    payment_type = Column(Integer, nullable=True)

    # Vehicle information
    vehicle_id_h = Column(String(16), nullable=False)

    # Audit fields
    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )
    updated_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
    )

    # Relationships
    pickup_zone = relationship(
        "Zone",
        foreign_keys=[pu_zone_id],
        back_populates="pickup_trips",
    )
    dropoff_zone = relationship(
        "Zone",
        foreign_keys=[do_zone_id],
        back_populates="dropoff_trips",
    )

    def __repr__(self) -> str:
        return (
            f"<CompleteTrip(trip_key='{self.trip_key}', "
            f"vendor_id={self.vendor_id}, "
            f"pickup_ts='{self.pickup_ts}', "
            f"dropoff_ts='{self.dropoff_ts}')>"
        )

    @property
    def trip_duration_minutes(self) -> Optional[float]:
        """Calculate trip duration in minutes."""
        if self.pickup_ts and self.dropoff_ts:
            delta = self.dropoff_ts - self.pickup_ts
            return delta.total_seconds() / 60
        return None

    @property
    def has_tip(self) -> bool:
        """Check if trip has a tip."""
        return self.tip_amount is not None and self.tip_amount > 0

    @property
    def is_complete(self) -> bool:
        """Check if trip has all required fields."""
        return all(
            [
                self.trip_key,
                self.vendor_id,
                self.pickup_ts,
                self.dropoff_ts,
                self.pu_zone_id,
                self.do_zone_id,
                self.vehicle_id_h,
            ]
        )
