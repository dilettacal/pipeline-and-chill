#!/usr/bin/env python3
"""
ChillFlow Batch Processing Demo

This script demonstrates the ChillFlow batch processing pipeline by:
1. Loading sample NYC taxi data into the database
2. Running batch aggregation to compute KPIs
3. Showing the results

Usage:
    uv run python scripts/demo/demo_chillflow_batch.py
"""

import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from batch import BatchAggregator, BatchTripProducer
from core import get_database_client, get_logger
from core.clients.database import get_db_session

logger = get_logger("demo.chillflow_batch")


def print_header(text: str):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def print_section(text: str):
    """Print a formatted section header."""
    print(f"\n{'─' * 80}")
    print(f"  {text}")
    print("─" * 80)


def check_infrastructure():
    """Check if required infrastructure is running."""
    print_section("🔍 Checking Infrastructure")

    try:
        # Test database connection
        from sqlalchemy import text

        client = get_database_client()
        session_gen = get_db_session()
        session = next(session_gen)
        session.execute(text("SELECT 1"))
        session.close()
        client.close()
        print("✅ Database connection successful")

        # Note: Redis and Kafka are not needed for batch processing
        # Batch processing only requires PostgreSQL for data storage
        print("ℹ️  Redis and Kafka not required for batch processing")

        return True
    except Exception as e:
        print(f"❌ Infrastructure check failed: {e}")
        return False


def load_sample_data():
    """Load a small sample of NYC taxi data into the database."""
    print_section("📊 Loading Sample Data")

    # Load January 2025 data (first 10,000 rows)
    data_file = (
        project_root / "data" / "raw" / "yellow" / "2025" / "01" / "yellow_tripdata_2025-01.parquet"
    )

    if not data_file.exists():
        print(f"❌ Data file not found: {data_file}")
        return False

    print(f"📂 Loading data from: {data_file}")
    df = pd.read_parquet(data_file)

    # Take a sample for demo (first 10,000 rows)
    sample_df = df.head(10000).copy()
    print(f"📊 Sample size: {len(sample_df):,} trips")

    # Process the data using MonthlyLoader functions
    from batch.loader import apply_dq_rules, compute_derived_fields, standardize_column_names

    # Step 1: Compute derived fields (using original column names)
    sample_df = compute_derived_fields(sample_df)

    # Step 2: Apply data quality rules (using original column names)
    sample_df, rejections = apply_dq_rules(sample_df)

    # Step 3: Standardize column names (rename to final schema)
    sample_df = standardize_column_names(sample_df)

    print(f"📊 After processing: {len(sample_df):,} valid trips")

    # Load into database using BatchTripProducer
    producer = BatchTripProducer()

    print("💾 Writing trips to database...")
    # Convert DataFrame to trips and write to database
    trips = producer._dataframe_to_trips(sample_df, source="demo")
    stats = producer._write_trips_batch(trips, batch_size=1000)

    print(f"✅ Loaded {stats['inserted']:,} trips")
    print(f"   - Inserted: {stats['inserted']:,}")
    print(f"   - Skipped: {stats['skipped']:,}")

    return True


def run_aggregation():
    """Run batch aggregation to compute KPIs."""
    print_section("🔄 Running Batch Aggregation")

    aggregator = BatchAggregator()

    # Get date range from the data
    client = get_database_client()
    session_gen = get_db_session()
    session = next(session_gen)

    try:
        from sqlalchemy import text

        result = session.execute(
            text(
                """
            SELECT
                MIN(pickup_ts) as min_ts,
                MAX(pickup_ts) as max_ts,
                COUNT(*) as trip_count
            FROM stg.complete_trip
        """
            )
        )
        row = result.fetchone()

        if not row or not row.trip_count:
            print("❌ No trip data found in database")
            return False

        print(f"📊 Database contains {row.trip_count:,} trips")
        print(f"📅 Date range: {row.min_ts} to {row.max_ts}")

        # Run aggregation for the full date range
        print(f"🔄 Running full refresh aggregation")

        stats = aggregator.aggregate_full_refresh()

        print(f"✅ Aggregation complete!")
        print(f"   - Hours processed: {stats['hours_processed']}")
        print(f"   - Zones affected: {stats['zones_affected']}")

        return True

    finally:
        session.close()
        client.close()


def show_results():
    """Show the aggregation results."""
    print_section("📈 Aggregation Results")

    aggregator = BatchAggregator()

    try:
        # Get KPI count
        kpi_count = aggregator.get_kpi_count()
        print(f"📊 Total KPI records: {kpi_count:,}")

        # Get date range
        date_range = aggregator.get_date_range()
        if date_range:
            print(f"📅 KPI date range: {date_range['min_hour']} to {date_range['max_hour']}")
        else:
            print("📅 No KPI data found")

        # Show sample KPIs
        client = get_database_client()
        session_gen = get_db_session()
        session = next(session_gen)

        try:
            from sqlalchemy import text

            result = session.execute(
                text(
                    """
                SELECT
                    zone_id,
                    hour_ts,
                    trips,
                    ROUND(avg_fare::numeric, 2) as avg_fare,
                    ROUND(avg_tip::numeric, 2) as avg_tip,
                    ROUND(avg_speed_kmh::numeric, 2) as avg_speed_kmh,
                    ROUND(avg_distance_km::numeric, 2) as avg_distance_km,
                    ROUND(pct_card::numeric, 1) as pct_card,
                    unique_vehicles
                FROM mart.zone_hourly_kpis
                ORDER BY hour_ts DESC, trips DESC
                LIMIT 10
            """
                )
            )

            rows = result.fetchall()
            if rows:
                print("\n🏆 Top 10 Zone-Hour KPIs:")
                print(
                    "Zone | Hour                | Trips | Avg Fare | Avg Tip | Speed | Distance | Card% | Vehicles"
                )
                print("─" * 100)
                for row in rows:
                    avg_fare = f"${row.avg_fare:.2f}" if row.avg_fare is not None else "N/A"
                    avg_tip = f"${row.avg_tip:.2f}" if row.avg_tip is not None else "N/A"
                    avg_speed = (
                        f"{row.avg_speed_kmh:.1f}" if row.avg_speed_kmh is not None else "N/A"
                    )
                    avg_distance = (
                        f"{row.avg_distance_km:.1f}" if row.avg_distance_km is not None else "N/A"
                    )
                    pct_card = f"{row.pct_card:.1f}" if row.pct_card is not None else "N/A"

                    print(
                        f"{row.zone_id:4} | {row.hour_ts} | {row.trips:5} | {avg_fare:>7} | {avg_tip:>6} | {avg_speed:>5} | {avg_distance:>8} | {pct_card:>5}% | {row.unique_vehicles:9}"
                    )
            else:
                print("❌ No KPI data found")

        finally:
            session.close()
            client.close()

    finally:
        aggregator.close()


def main():
    """Run the complete demo."""
    print_header("🚀 ChillFlow Batch Processing Demo")

    # Check infrastructure
    if not check_infrastructure():
        print("❌ Infrastructure not ready. Please start services with: make up")
        return 1

    # Load sample data
    if not load_sample_data():
        print("❌ Failed to load sample data")
        return 1

    # Run aggregation
    if not run_aggregation():
        print("❌ Failed to run aggregation")
        return 1

    # Show results
    show_results()

    print_header("✅ Demo Complete!")
    print("🎉 ChillFlow batch processing pipeline is working correctly!")
    print("\nNext steps:")
    print("  - Run 'uv run python -m batch aggregate status' to check KPIs")
    print("  - Explore the data with SQL queries")
    print("  - Build the stream processing service")

    return 0


if __name__ == "__main__":
    sys.exit(main())
