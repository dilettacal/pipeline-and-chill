# GitHub Actions Workflows

This directory contains GitHub Actions workflows for the ChillFlow platform.

## Workflows

### 1. `ci.yml` - Main Continuous Integration
- **Triggers**: Push to any branch, PRs to main/develop
- **Jobs**:
  - **Quality**: Code formatting, linting, pre-commit hooks
  - **Test**: Comprehensive test suite (unit, contract, smoke, performance)
  - **Integration**: Docker-based integration tests (on PRs and main branches)

### 2. `test.yml` - Test Suite Only
- **Triggers**: Push to any branch, PRs to main/develop
- **Focus**: Runs all test types with PostgreSQL and Redis services
- **Coverage**: Unit, stream, batch, contract, smoke, performance tests

### 3. `integration.yml` - Integration Tests Only
- **Triggers**: Push to main/develop, PRs to main/develop, manual dispatch
- **Focus**: Docker-based integration tests for stream and batch services
- **Requirements**: Docker for testcontainers

### 4. `quality.yml` - Code Quality Only
- **Triggers**: Push to any branch, PRs to main/develop
- **Focus**: Code formatting, linting, pre-commit hooks, Makefile validation

## Test Types

The workflows run different test categories:

- **Unit Tests**: Fast, isolated tests without external dependencies
- **Stream Tests**: Real-time processing component tests
- **Batch Tests**: Batch processing component tests
- **Contract Tests**: Data schema validation and transformations
- **Smoke Tests**: Lightweight end-to-end validation
- **Performance Tests**: Benchmarking critical components
- **Integration Tests**: Full service tests with Docker containers

## Environment Variables

The workflows set up the following environment variables:

```bash
DATABASE_URL=postgresql+psycopg://test:test@localhost:5432/test_chillflow
REDIS_URL=redis://localhost:6379
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
HASH_SALT=github-actions-test-salt
```

## Services

The workflows use the following services:

- **PostgreSQL 15**: Database for testing
- **Redis 7**: Cache for testing
- **Docker**: For testcontainers integration tests

## Artifacts

Test results and cache files are uploaded as artifacts with 7-day retention.
