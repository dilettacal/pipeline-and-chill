#!/usr/bin/env python3
"""
Test ChillFlow infrastructure connectivity.

This script tests all infrastructure components to ensure they're working correctly.
"""

import os
import sys
from datetime import datetime

# Add backend to path so we can import chillflow-core
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from chillflow import (
    CompleteTrip,
    CompleteTripSchema,
    Zone,
    get_database_client,
    get_kafka_client,
    get_logger,
    get_redis_client,
    setup_development_logging,
)
from chillflow.clients.database import get_db_session
from sqlalchemy import text


def test_database_connection():
    """Test PostgreSQL database connection."""
    print("🐘 Testing PostgreSQL connection...")

    try:
        db_client = get_database_client()

        # Test health check
        if not db_client.health_check():
            print("❌ Database health check failed")
            return False

        # Test connection info
        info = db_client.get_connection_info()
        print(f"✅ Database connected: {info['url']}")

        # Test session
        session_gen = get_db_session()
        session = next(session_gen)
        try:
            result = session.execute(text("SELECT 1 as test")).fetchone()
            if result and result[0] == 1:
                print("✅ Database query successful")
                return True
            else:
                print("❌ Database query failed")
                return False
        finally:
            session.close()

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def test_redis_connection():
    """Test Redis connection."""
    print("🔴 Testing Redis connection...")

    try:
        redis_client = get_redis_client()

        # Test health check
        if not redis_client.health_check():
            print("❌ Redis health check failed")
            return False

        # Test basic operations
        redis_client.set("test:chillflow", "infrastructure_test", ex=60)
        value = redis_client.get("test:chillflow")

        if value and value.decode() == "infrastructure_test":
            print("✅ Redis read/write successful")
            return True
        else:
            print(f"❌ Redis read/write failed: got {value}")
            return False

    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False


def test_kafka_connection():
    """Test Kafka connection."""
    print("📨 Testing Kafka connection...")

    try:
        kafka_client = get_kafka_client()

        # Test health check
        if not kafka_client.health_check():
            print("❌ Kafka health check failed")
            return False

        # Test producer
        producer = kafka_client.get_producer()
        success = producer.send_message(
            "test-topic",
            {
                "message": "ChillFlow infrastructure test",
                "timestamp": datetime.now().isoformat(),
            },
        )

        if success:
            print("✅ Kafka producer successful")
            return True
        else:
            print("❌ Kafka producer failed")
            return False

    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False


def test_database_models():
    """Test database models and schemas."""
    print("📊 Testing database models...")

    try:
        # Test schema validation
        trip_schema = CompleteTripSchema(
            trip_key="test_trip_123",
            vendor_id=1,
            pickup_ts=datetime.now(),
            dropoff_ts=datetime.now(),
            pu_zone_id=229,
            do_zone_id=230,
            vehicle_id_h="test_vehicle",
        )

        print(f"✅ Trip schema validation: {trip_schema.trip_key}")

        # Test database session with models
        session_gen = get_db_session()
        session = next(session_gen)
        try:
            # Test that we can query the database
            result = session.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dim'"
                )
            ).fetchone()
            dim_tables = result[0] if result else 0

            result = session.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'stg'"
                )
            ).fetchone()
            stg_tables = result[0] if result else 0

            print(f"✅ Database schemas found: dim={dim_tables}, stg={stg_tables}")
            return True
        finally:
            session.close()

    except Exception as e:
        print(f"❌ Database models test failed: {e}")
        return False


def main():
    """Run all infrastructure tests."""
    print("🚀 ChillFlow Infrastructure Test Suite")
    print("=" * 50)

    # Setup logging
    setup_development_logging("infrastructure-test")
    logger = get_logger("infrastructure-test")
    logger.info("Starting infrastructure tests")

    tests = [
        ("Database Connection", test_database_connection),
        ("Redis Connection", test_redis_connection),
        ("Kafka Connection", test_kafka_connection),
        ("Database Models", test_database_models),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        print("-" * 30)
        success = test_func()
        results.append((test_name, success))
        print()

    # Summary
    print("📊 Test Results Summary")
    print("=" * 50)
    passed = 0
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if success:
            passed += 1

    print(f"\n🎯 Results: {passed}/{len(results)} tests passed")

    if passed == len(results):
        print("🎉 All infrastructure tests passed! ChillFlow platform is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the infrastructure setup.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
