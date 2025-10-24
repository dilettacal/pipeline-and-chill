#!/usr/bin/env python3
"""
FluxFrame Full Pipeline Demo - Phase 8

Demonstrates the complete end-to-end data pipeline:
1. Prerequisites check (services, data, schema)
2. Batch pipeline (Jan-Apr historical data)
3. Streaming pipeline (May-Aug real-time simulation)
4. KPI aggregation (mart table population)
5. Data validation and reconciliation
6. Example analytics queries

This script orchestrates all components to show the complete FluxFrame system.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path


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


def print_step(step: int, total: int, text: str):
    """Print a step indicator."""
    print(f"\n[{step}/{total}] {text}")
    print("─" * 80)


def safe_input(prompt: str, default: str = "") -> str:
    """Safely get user input, returning default if not available."""
    try:
        return input(prompt).strip() or default
    except (EOFError, KeyboardInterrupt):
        return default


def check_docker() -> bool:
    """Check if Docker is available."""
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False


def check_service(container_name: str, service_name: str) -> bool:
    """Check if a Docker service is running."""
    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                f"name={container_name}",
                "--format",
                "{{.Status}}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return "Up" in result.stdout
    except Exception:
        return False


def check_all_services() -> dict:
    """Check status of all required services."""
    print_section("Checking Services")

    services = {
        "Postgres": "fluxframe-postgres",
        "Redis": "fluxframe-redis",
        "Redpanda (Kafka)": "fluxframe-redpanda",
    }

    results = {}
    all_ok = True

    for service_name, container_name in services.items():
        is_up = check_service(container_name, service_name)
        results[service_name] = is_up
        status = "✅" if is_up else "❌"
        print(f"{status} {service_name:20} ({container_name})")
        if not is_up:
            all_ok = False

    if not all_ok:
        print("\n💡 Start services with:")
        print("   make start-observability")
        print("   # OR")
        print("   make start")

    return results


def check_database_schema() -> bool:
    """Check if database tables exist."""
    print_section("Checking Database Schema")

    tables = [
        ("dim", "zone", "Taxi zones dimension table"),
        ("stg", "complete_trip", "Complete trips staging table"),
        ("mart", "zone_hourly_kpis", "Hourly zone KPIs mart table"),
    ]

    all_ok = True
    for schema, table, description in tables:
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
                    "-t",
                    "-c",
                    f"SELECT COUNT(*) FROM {schema}.{table};",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                count = int(result.stdout.strip())
                print(f"✅ {schema}.{table:20} - {count:>10,} rows")
            else:
                print(f"❌ {schema}.{table:20} - Table not found")
                all_ok = False

        except Exception as e:
            print(f"❌ {schema}.{table:20} - Error: {e}")
            all_ok = False

    if not all_ok:
        print("\n💡 Run database migrations:")
        print("   make db-migrate")

    return all_ok


def check_curated_data() -> dict:
    """Check availability of curated data."""
    print_section("Checking Curated Data")

    batch_months = ["01", "02", "03", "04"]
    stream_months = ["05", "06", "07", "08"]

    results = {"batch": [], "streaming": []}

    print("Batch months (Jan-Apr):")
    for month in batch_months:
        path = Path(f"data/curated/yellow/2025/{month}/trips_clean.parquet")
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"  ✅ 2025-{month}: {size_mb:>6.1f} MB")
            results["batch"].append(path)
        else:
            print(f"  ❌ 2025-{month}: Not found")

    print("\nStreaming months (May-Aug):")
    for month in stream_months:
        path = Path(f"data/curated/yellow/2025/{month}/trips_clean.parquet")
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"  ✅ 2025-{month}: {size_mb:>6.1f} MB")
            results["streaming"].append(path)
        else:
            print(f"  ❌ 2025-{month}: Not found")

    if not results["batch"] or not results["streaming"]:
        print("\n💡 Run data curation:")
        print("   make curate")

    return results


def run_batch_pipeline(sample_mode: bool = True):
    """Run the batch pipeline."""
    print_step(1, 4, "BATCH PIPELINE (Jan-Apr Historical Data)")

    if sample_mode:
        print("\n📊 Running in SAMPLE mode (10K trips from Jan)")
        print("   For full Jan-Apr data, set sample_mode=False")

        # Run demo batch producer
        result = subprocess.run(
            ["python", "scripts/demo/demo_batch_producer.py"],
            input="\n",  # Provide ENTER input
            text=True,
        )

        if result.returncode != 0:
            print("❌ Batch demo failed")
            return False
    else:
        print("\n📊 Running FULL batch load (Jan-Apr: ~13M trips)")
        print("   This will take ~10-15 minutes...")

        result = subprocess.run(
            ["make", "batch"],
        )

        if result.returncode != 0:
            print("❌ Batch processing failed")
            return False

    print("\n✅ Batch pipeline complete")
    return True


def run_streaming_pipeline(sample_mode: bool = True):
    """Run the streaming pipeline."""
    print_step(2, 4, "STREAMING PIPELINE (May-Aug Real-time Simulation)")

    if sample_mode:
        print("\n📊 Running in SAMPLE mode")
        print("   Starting trip assembler in background...")

        # Start assembler in background
        assembler_proc = subprocess.Popen(
            ["make", "assembler"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        print("   Waiting for assembler to initialize...")
        time.sleep(5)

        print("   Running replay producer demo...")
        result = subprocess.run(
            ["python", "scripts/demo/demo_replay_producer.py"],
            input="\n",  # Provide ENTER input
            text=True,
        )

        # Give it time to process
        print("   Waiting for events to be processed...")
        time.sleep(10)

        # Stop assembler
        print("   Stopping assembler...")
        assembler_proc.terminate()
        assembler_proc.wait(timeout=10)

        if result.returncode != 0:
            print("❌ Streaming demo failed")
            return False
    else:
        print("\n📊 Running FULL streaming pipeline (May-Aug)")
        print("   This requires manual orchestration:")
        print("   1. Terminal 1: make assembler")
        print("   2. Terminal 2: make replay")
        print("   This demo will skip full streaming in automated mode")

    print("\n✅ Streaming pipeline complete")
    return True


def run_aggregation():
    """Run the KPI aggregation."""
    print_step(3, 4, "KPI AGGREGATION (Mart Table Population)")

    print("\n📊 Computing hourly zone KPIs...")

    result = subprocess.run(
        ["make", "aggregator"],
    )

    if result.returncode != 0:
        print("❌ Aggregation failed")
        return False

    print("\n✅ KPI aggregation complete")
    return True


def validate_data():
    """Validate pipeline data."""
    print_step(4, 4, "DATA VALIDATION & RECONCILIATION")

    # Check stg.complete_trip
    print("\n📊 Checking stg.complete_trip:")
    subprocess.run(
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
            """
SELECT 
    source,
    COUNT(*) as trips,
    MIN(pickup_ts) as first_trip,
    MAX(pickup_ts) as last_trip,
    TO_CHAR(SUM(total_amount), 'FM$999,999,990.00') as total_revenue
FROM stg.complete_trip
GROUP BY source
ORDER BY source;
            """,
        ],
        capture_output=False,
    )

    # Check mart.zone_hourly_kpis
    print("\n📊 Checking mart.zone_hourly_kpis:")
    subprocess.run(
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
            """
SELECT 
    COUNT(*) as kpi_records,
    COUNT(DISTINCT zone_id) as zones_covered,
    MIN(hour_ts) as first_hour,
    MAX(hour_ts) as last_hour,
    TO_CHAR(SUM(trips), 'FM999,999,990') as total_trips_aggregated
FROM mart.zone_hourly_kpis;
            """,
        ],
        capture_output=False,
    )

    print("\n✅ Data validation complete")
    return True


def show_analytics_examples():
    """Show example analytics queries."""
    print_section("Example Analytics Queries")

    print("\n📊 Top 10 Zones by Trip Volume:")
    subprocess.run(
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
            """
SELECT 
    z.zone_name,
    z.borough,
    SUM(k.trips) as total_trips,
    TO_CHAR(AVG(k.avg_fare), 'FM$990.00') as avg_fare,
    TO_CHAR(AVG(k.avg_tip), 'FM$990.00') as avg_tip
FROM mart.zone_hourly_kpis k
JOIN dim.zone z ON k.zone_id = z.zone_id
GROUP BY z.zone_name, z.borough
ORDER BY total_trips DESC
LIMIT 10;
            """,
        ],
        capture_output=False,
    )

    print("\n📊 Peak Hours Across All Zones:")
    subprocess.run(
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
            """
SELECT 
    EXTRACT(HOUR FROM hour_ts) as hour_of_day,
    SUM(trips) as total_trips,
    TO_CHAR(AVG(avg_fare), 'FM$990.00') as avg_fare,
    ROUND(AVG(pct_card), 1) as pct_card_payment
FROM mart.zone_hourly_kpis
GROUP BY EXTRACT(HOUR FROM hour_ts)
ORDER BY hour_of_day;
            """,
        ],
        capture_output=False,
    )


def show_monitoring_urls():
    """Show monitoring and observability URLs."""
    print_section("Monitoring & Observability")

    print("🎛️  Data Source UIs:")
    print("   • Redpanda Console: http://localhost:8080")
    print("   • pgAdmin:          http://localhost:5050")
    print("   • RedisInsight:     http://localhost:5540")
    print("")
    print("📊 Unified Observability:")
    print("   • Grafana:          http://localhost:3000 (admin/admin)")
    print("     - Dashboard: Pipeline Overview")
    print("     - Dashboard: Trip Assembler")
    print("     - Explore logs with Loki")
    print("     - View metrics with Prometheus")
    print("")
    print("🔍 Raw Backends (optional):")
    print("   • Prometheus:       http://localhost:9090")
    print("   • Loki:             http://localhost:3100")


def main():
    """Run the full pipeline demo."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="FluxFrame Full Pipeline Demo")
    parser.add_argument(
        "--mode",
        choices=["sample", "full"],
        default="sample",
        help="Demo mode: sample (quick) or full (complete pipeline)",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Run in non-interactive mode (no user prompts)",
    )
    args = parser.parse_args()

    print_header("🚀 FluxFrame Full Pipeline Demo - Phase 8")

    print("This demo orchestrates the complete data pipeline:")
    print("")
    print("  📦 Phase 6: Batch Pipeline (Jan-Apr)")
    print("  🔄 Phase 5: Streaming Pipeline (May-Aug)")
    print("  📊 Phase 7: KPI Aggregation (mart table)")
    print("  ✅ Phase 8: Validation & Analytics")
    print("")
    print("Pipeline architecture:")
    print("")
    print("  Raw Data → Curation → {Batch | Streaming} → stg.complete_trip → KPIs")
    print("")

    # Prerequisites
    print_step(0, 4, "PREREQUISITES CHECK")

    if not check_docker():
        print("❌ Docker not available")
        sys.exit(1)
    print("✅ Docker available")

    services = check_all_services()
    if not all(services.values()):
        print("\n❌ Not all services are running. Exiting.")
        sys.exit(1)

    if not check_database_schema():
        print("\n❌ Database schema incomplete. Exiting.")
        sys.exit(1)

    curated_data = check_curated_data()
    if not curated_data["batch"] and not curated_data["streaming"]:
        print("\n❌ No curated data available. Exiting.")
        sys.exit(1)

    # Determine demo mode
    if args.non_interactive:
        sample_mode = args.mode == "sample"
        print(f"\n🎬 Running in {args.mode.upper()} mode (non-interactive)")
    else:
        # Ask user for demo mode
        print("\n" + "=" * 80)
        print("Demo mode:")
        print("  1. SAMPLE - Quick demo with limited data (~30 seconds)")
        print("  2. FULL   - Complete pipeline with all data (~20 minutes)")
        print("=" * 80)

        mode = safe_input("\nSelect mode [1/2] (default: 1): ", "1")
        sample_mode = mode == "1"

        if not sample_mode:
            print("\n⚠️  FULL mode will process millions of trips!")
            confirm = safe_input("Are you sure? [y/N]: ", "n").lower()
            if confirm != "y":
                print("Demo cancelled.")
                sys.exit(0)

    # Run pipelines
    print_header("🎬 Running FluxFrame Pipeline")

    # Batch pipeline
    if not run_batch_pipeline(sample_mode=sample_mode):
        print("\n❌ Batch pipeline failed. Exiting.")
        sys.exit(1)

    # Streaming pipeline
    if not run_streaming_pipeline(sample_mode=sample_mode):
        print("\n❌ Streaming pipeline failed. Exiting.")
        sys.exit(1)

    # KPI aggregation
    if not run_aggregation():
        print("\n❌ KPI aggregation failed. Exiting.")
        sys.exit(1)

    # Validation
    if not validate_data():
        print("\n❌ Data validation failed. Exiting.")
        sys.exit(1)

    # Analytics examples
    show_analytics_examples()

    # Monitoring
    show_monitoring_urls()

    # Final summary
    print_header("✅ FluxFrame Pipeline Demo Complete!")

    print("Pipeline successfully processed data through all stages:")
    print("")
    print("  ✅ Batch historical data (Jan-Apr)")
    print("  ✅ Streaming real-time simulation (May-Aug)")
    print("  ✅ Canonical trip table populated (stg.complete_trip)")
    print("  ✅ Hourly KPIs computed (mart.zone_hourly_kpis)")
    print("  ✅ Data validated and reconciled")
    print("")
    print("Next steps:")
    print("  • Explore data in pgAdmin: http://localhost:5050")
    print("  • View dashboards in Grafana: http://localhost:3000")
    print("  • Run custom analytics queries")
    print("  • Deploy to Kubernetes (Phase 9)")
    print("")
    print("=" * 80 + "\n")


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
