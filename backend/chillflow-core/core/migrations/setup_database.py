#!/usr/bin/env python3
"""
Database setup utility for testing and development.

This module provides functions to set up the database schema and seed data
for testing purposes.
"""
import os
from pathlib import Path
from typing import Optional

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def setup_database_schema(engine: Engine) -> None:
    """
    Set up the database schema using Alembic migrations.

    Args:
        engine: SQLAlchemy engine connected to the database
    """
    # Get the alembic config
    project_root = Path(__file__).parent.parent.parent.parent.parent
    alembic_cfg_path = (
        project_root / "backend" / "chillflow-core" / "core" / "migrations" / "alembic.ini"
    )

    # Create alembic config
    alembic_cfg = Config(str(alembic_cfg_path))

    # Set the database URL in the config
    database_url = str(engine.url)
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)

    # Run migrations to head
    command.upgrade(alembic_cfg, "head")


def seed_zones_data(engine: Engine, csv_path: Optional[Path] = None) -> None:
    """
    Seed the dim.zone table with taxi zone data.

    Args:
        engine: SQLAlchemy engine connected to the database
        csv_path: Optional path to the CSV file. If not provided, uses default location.
    """
    if csv_path is None:
        project_root = Path(__file__).parent.parent.parent.parent.parent
        csv_path = project_root / "data" / "raw" / "zones" / "taxi_zone_lookup.csv"

    if not csv_path.exists():
        print(f"⚠️  CSV file not found: {csv_path}")
        print("   Creating minimal test data instead...")
        _create_minimal_zone_data(engine)
        return

    import csv

    with engine.connect() as conn:
        with conn.begin():
            # Check if already seeded
            result = conn.execute(text("SELECT COUNT(*) FROM dim.zone"))
            count = result.scalar()

            if count > 0:
                print(f"⚠️  dim.zone already contains {count} rows. Skipping seed.")
                return

            # Read CSV and insert rows
            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                rows_inserted = 0

                for row in reader:
                    conn.execute(
                        text(
                            """
                        INSERT INTO dim.zone (zone_id, borough, zone_name, service_zone)
                        VALUES (:zone_id, :borough, :zone_name, :service_zone)
                        """
                        ),
                        {
                            "zone_id": int(row["LocationID"]),
                            "borough": row["Borough"],
                            "zone_name": row["Zone"],
                            "service_zone": row["service_zone"],
                        },
                    )
                    rows_inserted += 1

            print(f"✅ Inserted {rows_inserted} zones into dim.zone")


def _create_minimal_zone_data(engine: Engine) -> None:
    """Create minimal zone data for testing when CSV is not available."""
    with engine.connect() as conn:
        with conn.begin():
            # Check if already seeded
            result = conn.execute(text("SELECT COUNT(*) FROM dim.zone"))
            count = result.scalar()

            if count > 0:
                return

            # Insert minimal test data
            test_zones = [
                (1, "Manhattan", "Central Park", "Yellow Zone"),
                (2, "Manhattan", "Times Square", "Yellow Zone"),
                (3, "Brooklyn", "Brooklyn Heights", "Boro Zone"),
                (4, "Queens", "LaGuardia Airport", "Airport"),
                (5, "Queens", "JFK Airport", "Airport"),
            ]

            for zone_id, borough, zone_name, service_zone in test_zones:
                conn.execute(
                    text(
                        """
                    INSERT INTO dim.zone (zone_id, borough, zone_name, service_zone)
                    VALUES (:zone_id, :borough, :zone_name, :service_zone)
                    """
                    ),
                    {
                        "zone_id": zone_id,
                        "borough": borough,
                        "zone_name": zone_name,
                        "service_zone": service_zone,
                    },
                )

            print(f"✅ Inserted {len(test_zones)} test zones into dim.zone")


def setup_test_database(engine: Engine, seed_zones: bool = True) -> None:
    """
    Complete database setup for testing.

    This function:
    1. Sets up the database schema using Alembic migrations
    2. Optionally seeds the zone data

    Args:
        engine: SQLAlchemy engine connected to the database
        seed_zones: Whether to seed the zone data
    """
    print("🔧 Setting up database schema...")
    setup_database_schema(engine)

    if seed_zones:
        print("🌱 Seeding zone data...")
        seed_zones_data(engine)

    print("✅ Database setup complete!")


if __name__ == "__main__":
    # Example usage
    database_url = os.getenv("DATABASE_URL", "postgresql://dev:dev@localhost:5432/chillflow")
    engine = create_engine(database_url)
    setup_test_database(engine)
