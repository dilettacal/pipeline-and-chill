#!/usr/bin/env python3
"""
Seed dim.zone table with NYC taxi zone data.

Usage:
    python seed_zones.py
"""
import csv
import os
from pathlib import Path

import psycopg


def seed_zones():
    """Load taxi zone lookup data into dim.zone table."""

    # Database connection
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://dev:dev@localhost:5432/chillflow"
    )

    # Path to CSV file
    project_root = Path(__file__).parent.parent.parent.parent.parent
    csv_path = project_root / "data" / "raw" / "zones" / "taxi_zone_lookup.csv"

    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        print(f"   Please run: ./scripts/download_reference_data.sh")
        return

    print(f"📁 Reading zones from: {csv_path}")

    # Connect to database
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            # Check if already seeded
            cur.execute("SELECT COUNT(*) FROM dim.zone")
            count = cur.fetchone()[0]

            if count > 0:
                print(f"⚠️  dim.zone already contains {count} rows. Skipping seed.")
                print(f"   To reseed, run: DELETE FROM dim.zone;")
                return

            # Read CSV and insert rows
            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                rows_inserted = 0

                for row in reader:
                    cur.execute(
                        """
                        INSERT INTO dim.zone (zone_id, borough, zone_name, service_zone)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            int(row["LocationID"]),
                            row["Borough"],
                            row["Zone"],
                            row["service_zone"],
                        ),
                    )
                    rows_inserted += 1

            conn.commit()
            print(f"✅ Inserted {rows_inserted} zones into dim.zone")

    # Verify
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dim.zone")
            count = cur.fetchone()[0]
            print(f"✅ Verification: dim.zone now contains {count} rows")

            # Show sample
            cur.execute("SELECT zone_id, borough, zone_name FROM dim.zone LIMIT 5")
            print("\n📋 Sample zones:")
            for row in cur.fetchall():
                print(f"   {row[0]:3d} | {row[1]:15s} | {row[2]}")


if __name__ == "__main__":
    seed_zones()
