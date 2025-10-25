"""Test that required dependencies are available."""

import pytest


def test_psycopg_available():
    """Test that psycopg is available."""
    try:
        import psycopg

        assert True
    except ImportError:
        pytest.fail("psycopg not available")


def test_sqlalchemy_available():
    """Test that SQLAlchemy is available."""
    try:
        from sqlalchemy import create_engine

        assert True
    except ImportError:
        pytest.fail("SQLAlchemy not available")


def test_testcontainers_available():
    """Test that testcontainers is available."""
    try:
        from testcontainers.postgres import PostgresContainer

        assert True
    except ImportError:
        pytest.fail("testcontainers not available")
