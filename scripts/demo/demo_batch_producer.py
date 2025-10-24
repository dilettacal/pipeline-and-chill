#!/usr/bin/env python3
"""
Demo script for Batch Trip Producer.

This script demonstrates the batch producer functionality by:
1. Checking prerequisites (Postgres running, curated data)
2. Running a small sample batch load (1 month subset)
3. Showing database statistics
4. Verifying data was loaded correctly
"""

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add package paths
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "packages" / "core-utils"))
sys.path.insert(0, str(project_root / "packages" / "batch-trip-producer"))

from fluxframe.processor import BatchTripProcessor
from fluxframe.settings import settings


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


def check_postgres() -> bool:
    """Check if Postgres is running and accessible."""
    print_section("1. Checking Postgres Connection")

    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "fluxframe-postgres",
                "psql",
                "-U",
                "dev",
                "-d",
                "fluxframe",
                "-c",
                "SELECT 1;",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            print(f"✅ Postgres is running")
            print(f"   Database URL: {settings.DATABASE_URL}")
            return True
        else:
            print(f"❌ Postgres connection failed")
            print(f"   Error: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        print("❌ Timeout connecting to Postgres")
        return False
    except FileNotFoundError:
        print("❌ Docker not available")
        return False
    except Exception as e:
        print(f"❌ Error connecting to Postgres: {e}")
        print("\n💡 Start Postgres with:")
        print("   make start")
        print("   # OR")
        print("   docker compose -f infra/docker-compose.yml up -d")
        return False


def check_table_exists() -> bool:
    """Check if stg.complete_trip table exists."""
    print_section("2. Checking Database Schema")

    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "fluxframe-postgres",
                "psql",
                "-U",
                "dev",
                "-d",
                "fluxframe",
                "-c",
                "\\dt stg.complete_trip",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if "complete_trip" in result.stdout:
            print("✅ Table stg.complete_trip exists")

            # Get row count
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "fluxframe-postgres",
                    "psql",
                    "-U",
                    "dev",
                    "-d",
                    "fluxframe",
                    "-t",
                    "-c",
                    "SELECT COUNT(*) FROM stg.complete_trip;",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            count = int(result.stdout.strip())
            print(f"   Current row count: {count:,}")

            return True
        else:
            print("❌ Table stg.complete_trip not found")
            print("\n💡 Run database migrations:")
            print("   make db-migrate")
            return False

    except Exception as e:
        print(f"⚠️  Error checking table: {e}")
        return False


def check_curated_data() -> tuple[bool, list[Path]]:
    """Check if curated data exists for Jan-Apr."""
    print_section("3. Checking Curated Data (Jan-Apr)")

    data_paths = []
    months = ["01", "02", "03", "04"]

    for month in months:
        data_path = Path(f"data/curated/yellow/2025/{month}/trips_clean.parquet")

        if data_path.exists():
            size_mb = data_path.stat().st_size / (1024 * 1024)
            df = pd.read_parquet(data_path)
            print(f"✅ 2025-{month}: {len(df):>10,} rows ({size_mb:>6.1f} MB)")
            data_paths.append(data_path)
        else:
            print(f"❌ 2025-{month}: Not found at {data_path}")

    if not data_paths:
        print("\n💡 Run data curation first:")
        print("   make curate")
        print("   # OR")
        print("   cd packages/ingestion-monthly-loader")
        print("   python -m fluxframe.cli --all")
        return False, []

    print(f"\n📊 Total: {len(data_paths)} months available")
    return True, data_paths


def show_sample_data(data_paths: list[Path]):
    """Show sample of curated data."""
    print_section("4. Sample Curated Data")

    # Read first file
    df = pd.read_parquet(data_paths[0])

    print(f"Columns ({len(df.columns)}):")
    for col in df.columns:
        print(f"  • {col}")

    print(f"\nFirst trip:")
    trip = df.iloc[0]
    print(f"  Pickup:   {trip['pickup_ts']}")
    print(f"  Dropoff:  {trip['dropoff_ts']}")
    print(f"  Vendor:   {trip['vendor_id']}")
    print(f"  Zone:     {trip['pu_zone_id']} → {trip['do_zone_id']}")
    print(f"  Distance: {trip['distance_km']:.2f} km")
    print(f"  Duration: {trip['duration_min']:.2f} min")
    print(f"  Fare:     ${trip['fare_amount']:.2f}")
    print(f"  Tip:      ${trip['tip_amount']:.2f}")
    print(f"  Total:    ${trip['total_amount']:.2f}")


def run_small_batch_load(data_path: Path, sample_size: int = 10000) -> dict:
    """Run a small batch load with limited trips."""
    print_section(f"5. Running Small Batch Load ({sample_size:,} trips)")

    # Read sample data
    print(f"\nReading {sample_size:,} trips from {data_path.name}...")
    df = pd.read_parquet(data_path)
    original_count = len(df)
    df = df.head(sample_size)  # Take only first N rows
    print(f"✅ Loaded {len(df):,} of {original_count:,} trips")

    # Create temporary parquet file
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write sample to temp file
        df.to_parquet(temp_path, engine="pyarrow", index=False)

        # Initialize processor
        print(f"\nInitializing batch processor...")
        processor = BatchTripProcessor(
            database_url=settings.DATABASE_URL, hash_salt=settings.HASH_SALT
        )

        # Process the file
        print(f"\nProcessing and loading to database...")
        start_time = time.time()
        stats = processor.process_month(temp_path, batch_size=1000, source="batch-demo")
        duration = time.time() - start_time

        processor.close()

        print(f"\n✅ Batch load complete!")
        print(f"   Rows read:    {stats['rows_read']:,}")
        print(f"   Inserted:     {stats['inserted']:,}")
        print(f"   Skipped:      {stats['skipped']:,}")
        print(f"   Duration:     {duration:.2f}s")
        print(f"   Rate:         {stats['rows_read'] / duration:.0f} trips/sec")

        return stats

    finally:
        # Cleanup temp file
        temp_path.unlink(missing_ok=True)


def verify_database_load():
    """Verify data was loaded correctly."""
    print_section("6. Verifying Database Load")

    try:
        # Total count
        result = subprocess.run(
            [
                "docker",
                "exec",
                "fluxframe-postgres",
                "psql",
                "-U",
                "dev",
                "-d",
                "fluxframe",
                "-t",
                "-c",
                "SELECT COUNT(*) FROM stg.complete_trip;",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        total_count = int(result.stdout.strip())
        print(f"✅ Total trips in database: {total_count:,}")

        # Count by source
        result = subprocess.run(
            [
                "docker",
                "exec",
                "fluxframe-postgres",
                "psql",
                "-U",
                "dev",
                "-d",
                "fluxframe",
                "-c",
                "SELECT source, COUNT(*) as count FROM stg.complete_trip GROUP BY source ORDER BY source;",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        print(f"\nBreakdown by source:")
        print(result.stdout)

        # Sample trips
        result = subprocess.run(
            [
                "docker",
                "exec",
                "fluxframe-postgres",
                "psql",
                "-U",
                "dev",
                "-d",
                "fluxframe",
                "-c",
                "SELECT trip_key, vendor_id, pickup_ts, pu_zone_id, do_zone_id, fare_amount, source FROM stg.complete_trip WHERE source='batch-demo' LIMIT 3;",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        print(f"\nSample trips (first 3):")
        print(result.stdout)

    except Exception as e:
        print(f"⚠️  Error verifying database: {e}")


def show_full_batch_command():
    """Show commands for full batch load."""
    print_section("7. Full Batch Load Commands")

    print("To load all Jan-Apr data (~13M trips):")
    print("")
    print("  # Process all months:")
    print("  make batch")
    print("")
    print("  # OR process one month at a time:")
    print("  cd packages/batch-trip-producer")
    print("  python -m fluxframe.cli process-month --month 2025-01")
    print("  python -m fluxframe.cli process-month --month 2025-02")
    print("  python -m fluxframe.cli process-month --month 2025-03")
    print("  python -m fluxframe.cli process-month --month 2025-04")
    print("")
    print("  # Check configuration:")
    print("  make batch-info")
    print("")
    print("Expected results:")
    print("  • Jan-Apr: ~13.2M trips (batch source)")
    print("  • Processing time: ~7-10 minutes total")
    print("  • No duplicates (idempotent)")


def cleanup_demo_data():
    """Offer to clean up demo data."""
    print_section("8. Cleanup (Optional)")

    print("To remove demo data from database:")
    print("  docker exec fluxframe-postgres psql -U dev -d fluxframe -c \\")
    print("    \"DELETE FROM stg.complete_trip WHERE source='batch-demo';\"")
    print("")
    print("To reset entire pipeline:")
    print("  make clean-pipeline")


def main():
    """Run the demo."""
    print_header("📦 FluxFrame Batch Trip Producer Demo")

    print("This demo will:")
    print("  1. Check prerequisites (Postgres, curated data)")
    print("  2. Show sample curated data")
    print("  3. Run a small batch load (10,000 trips)")
    print("  4. Verify data in database")
    print("  5. Show commands for full batch load")

    # Check prerequisites
    pg_ok = check_postgres()
    if not pg_ok:
        print("\n❌ Cannot proceed without Postgres. Exiting.")
        sys.exit(1)

    table_ok = check_table_exists()
    if not table_ok:
        print("\n❌ Cannot proceed without database schema. Exiting.")
        sys.exit(1)

    data_ok, data_paths = check_curated_data()
    if not data_ok:
        print("\n❌ Cannot proceed without curated data. Exiting.")
        sys.exit(1)

    # Show sample data
    show_sample_data(data_paths)

    # Wait for user
    print("\n" + "=" * 80)
    input("Press ENTER to run the small batch load (10,000 trips)... ")

    # Run small batch load (use first month)
    stats = run_small_batch_load(data_paths[0], sample_size=10000)

    # Small delay
    time.sleep(1)

    # Verify database
    verify_database_load()

    # Show full batch commands
    show_full_batch_command()

    # Cleanup info
    cleanup_demo_data()

    # Final message
    print_header("✅ Demo Complete!")

    print("Key differences from streaming path:")
    print("  ✅ No Kafka/Redis complexity")
    print("  ✅ Direct database writes (much faster)")
    print("  ✅ Batch inserts for performance")
    print("  ✅ Idempotent (re-run safe)")
    print("  ✅ Simple architecture")

    print("\nNext steps:")
    print("  • Load full Jan-Apr data: make batch")
    print("  • Check pgAdmin: http://localhost:5050")
    print("  • Verify data: make db-shell")
    print("  • Build KPI Mart (Phase 7)")

    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
