#!/usr/bin/env python3
"""
Generate analytics reports from processed data.

This script creates summary reports and exports data for analysis.
"""

import sys
from pathlib import Path

# Add the backend to the Python path
backend_path = Path(__file__).parent.parent.parent / "backend"
sys.path.insert(0, str(backend_path))

from core import get_database_client, get_db_session, get_logger
from sqlalchemy import text

logger = get_logger("analytics.reports")


def generate_reports():
    """Generate analytics reports."""
    logger.info("Generating analytics reports...")

    # Get database connection
    client = get_database_client()
    session_gen = get_db_session()
    session = next(session_gen)

    try:
        # Generate summary statistics
        logger.info("Computing summary statistics...")

        # Trip statistics
        trip_stats = session.execute(
            text(
                """
            SELECT
                COUNT(*) as total_trips,
                COUNT(DISTINCT pu_zone_id) as pickup_zones,
                COUNT(DISTINCT do_zone_id) as dropoff_zones,
                AVG(fare_amount) as avg_fare,
                AVG(tip_amount) as avg_tip,
                AVG(total_amount) as avg_total,
                MIN(pickup_ts) as earliest_trip,
                MAX(pickup_ts) as latest_trip
            FROM stg.complete_trip
        """
            )
        ).fetchone()

        # KPI statistics
        kpi_stats = session.execute(
            text(
                """
            SELECT
                COUNT(*) as total_kpis,
                COUNT(DISTINCT zone_id) as zones_with_kpis,
                AVG(trips) as avg_trips_per_hour,
                AVG(avg_fare) as avg_fare_per_hour,
                AVG(avg_tip) as avg_tip_per_hour
            FROM mart.zone_hourly_kpis
        """
            )
        ).fetchone()

        # Zone statistics
        zone_stats = session.execute(
            text(
                """
            SELECT
                COUNT(*) as total_zones,
                COUNT(DISTINCT borough) as boroughs,
                COUNT(DISTINCT service_zone) as service_zones
            FROM dim.zone
        """
            )
        ).fetchone()

        # Print report
        print("\n" + "=" * 60)
        print("📊 CHILLFLOW ANALYTICS REPORT")
        print("=" * 60)

        print(f"\n🚕 TRIP STATISTICS:")
        print(f"   Total Trips: {trip_stats.total_trips:,}")
        print(f"   Pickup Zones: {trip_stats.pickup_zones:,}")
        print(f"   Dropoff Zones: {trip_stats.dropoff_zones:,}")
        print(f"   Avg Fare: ${trip_stats.avg_fare:.2f}")
        print(f"   Avg Tip: ${trip_stats.avg_tip:.2f}")
        print(f"   Avg Total: ${trip_stats.avg_total:.2f}")
        print(f"   Time Range: {trip_stats.earliest_trip} to {trip_stats.latest_trip}")

        print(f"\n📈 KPI STATISTICS:")
        print(f"   Total KPIs: {kpi_stats.total_kpis:,}")
        print(f"   Zones with KPIs: {kpi_stats.zones_with_kpis:,}")
        print(f"   Avg Trips/Hour: {kpi_stats.avg_trips_per_hour:.1f}")
        print(f"   Avg Fare/Hour: ${kpi_stats.avg_fare_per_hour:.2f}")
        print(f"   Avg Tip/Hour: ${kpi_stats.avg_tip_per_hour:.2f}")

        print(f"\n🌍 ZONE STATISTICS:")
        print(f"   Total Zones: {zone_stats.total_zones:,}")
        print(f"   Boroughs: {zone_stats.boroughs:,}")
        print(f"   Service Zones: {zone_stats.service_zones:,}")

        # Top zones by trip count
        print(f"\n🏆 TOP 10 ZONES BY TRIP COUNT:")
        top_zones = session.execute(
            text(
                """
            SELECT
                z.zone_name,
                z.borough,
                COUNT(*) as trip_count,
                AVG(ct.fare_amount) as avg_fare
            FROM stg.complete_trip ct
            JOIN dim.zone z ON ct.pu_zone_id = z.zone_id
            GROUP BY z.zone_id, z.zone_name, z.borough
            ORDER BY trip_count DESC
            LIMIT 10
        """
            )
        ).fetchall()

        for i, zone in enumerate(top_zones, 1):
            print(
                f"   {i:2d}. {zone.zone_name} ({zone.borough}): {zone.trip_count:,} trips, avg ${zone.avg_fare:.2f}"
            )

        print("\n" + "=" * 60)
        print("✅ Report generation complete!")
        print("=" * 60)

    finally:
        session.close()
        client.close()


if __name__ == "__main__":
    generate_reports()
