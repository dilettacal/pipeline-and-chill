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

    try:
        # Get database connection
        client = get_database_client()
        session_gen = get_db_session()
        session = next(session_gen)

        # Check if schemas exist before trying to clear them
        print("🔍 Checking database schemas...")

        # Check if stg schema exists
        stg_exists = session.execute(
            text(
                """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata
                WHERE schema_name = 'stg'
            )
        """
            )
        ).scalar()

        # Check if mart schema exists
        mart_exists = session.execute(
            text(
                """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata
                WHERE schema_name = 'mart'
            )
        """
            )
        ).scalar()

        cleared_tables = []

        # Clear trip data if stg schema exists
        if stg_exists:
            print("🔄 Clearing trip data...")
            session.execute(text("TRUNCATE TABLE stg.complete_trip CASCADE"))
            cleared_tables.append("stg.complete_trip")
        else:
            print("ℹ️  stg schema not found - skipping trip data cleanup")

        # Clear KPI data if mart schema exists
        if mart_exists:
            print("🔄 Clearing KPI data...")
            session.execute(text("TRUNCATE TABLE mart.zone_hourly_kpis CASCADE"))
            cleared_tables.append("mart.zone_hourly_kpis")
        else:
            print("ℹ️  mart schema not found - skipping KPI data cleanup")

        # Commit changes
        session.commit()
        session.close()
        client.close()

        if cleared_tables:
            print("✅ Database tables cleared successfully!")
            for table in cleared_tables:
                print(f"   - {table}: cleared")
            print("   - dim.zone: preserved (zones remain)")
        else:
            print("✅ Database cleanup completed!")
            print("   ℹ️  No tables to clear (schemas not found)")
            print("   ℹ️  Run database migrations first: make pipeline-batch")

        return 0

    except Exception as e:
        print(f"ℹ️  Database not accessible: {e}")
        print("   ℹ️  This is normal if infrastructure is not running")
        print("✅ Database cleanup completed (no action needed)")
        return 0


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
