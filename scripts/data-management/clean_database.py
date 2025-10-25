#!/usr/bin/env python3
"""
Clean database tables while keeping infrastructure running.

This script clears trip data and KPIs but preserves zones and infrastructure.
"""

import sys

from core import get_database_client, get_db_session
from sqlalchemy import text


def clean_database():
    """Clear database tables while keeping infrastructure running."""
    print("🗑️  Clearing database tables...")
    print("⚠️  Make sure infrastructure is running: make up")

    try:
        # Get database connection
        client = get_database_client()
        session_gen = get_db_session()
        session = next(session_gen)

        # Clear trip data and KPIs (preserve zones)
        print("🔄 Clearing trip data...")
        session.execute(text("TRUNCATE TABLE stg.complete_trip CASCADE"))

        print("🔄 Clearing KPI data...")
        session.execute(text("TRUNCATE TABLE mart.zone_hourly_kpis CASCADE"))

        # Commit changes
        session.commit()
        session.close()
        client.close()

        print("✅ Database tables cleared successfully!")
        print("   - stg.complete_trip: cleared")
        print("   - mart.zone_hourly_kpis: cleared")
        print("   - dim.zone: preserved (zones remain)")

        return 0

    except Exception as e:
        print(f"❌ Error clearing database: {e}")
        print("   Make sure infrastructure is running: make up")
        return 1


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Clean database tables")
    parser.add_argument("--confirm", action="store_true", help="Skip confirmation prompt")

    args = parser.parse_args()

    if not args.confirm:
        print("⚠️  This will clear all trip data and KPIs from the database!")
        print("   - stg.complete_trip will be cleared")
        print("   - mart.zone_hourly_kpis will be cleared")
        print("   - dim.zone will be preserved")
        print()
        response = input("Continue? (y/N): ").strip().lower()
        if response not in ["y", "yes"]:
            print("❌ Cancelled")
            return 1

    return clean_database()


if __name__ == "__main__":
    sys.exit(main())
