"""
Pytest configuration for ChillFlow tests.

This module provides shared fixtures and configuration for all test suites.
"""

import sys
from pathlib import Path

import pytest

# Add backend to Python path for imports
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))


@pytest.fixture(scope="session")
def backend_path():
    """Get the backend directory path."""
    return Path(__file__).parent.parent / "backend"


@pytest.fixture(scope="session")
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent


# Markers for different test types
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "unit: Unit tests that don't require external dependencies")
    config.addinivalue_line(
        "markers", "integration: Integration tests that require external services"
    )
    config.addinivalue_line("markers", "infrastructure: Infrastructure tests that require Docker")
    config.addinivalue_line("markers", "e2e: End-to-end tests that require full platform")
    config.addinivalue_line(
        "markers", "contract: Contract tests for data schemas and transformations"
    )
    config.addinivalue_line("markers", "smoke: Smoke tests for lightweight end-to-end validation")
    config.addinivalue_line("markers", "performance: Performance tests and benchmarks")
