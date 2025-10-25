# Quick Start Guide

## 🚀 Getting Started

This guide will help you get the ChillFlow pipeline running locally in minutes.

## 📋 Prerequisites

- **Python 3.12+**
- **Docker & Docker Compose**
- **uv** (Python package manager)
- **Git**

## 🏗️ Local Development Setup

### 1. **Clone the Repository**
```bash
git clone <repository-url>
cd pipeline-and-chill
```

### 2. **Install Dependencies**
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

### 3. **Start Infrastructure**
```bash
# Start all required services (PostgreSQL, Redis, Kafka)
make env up

# Check infrastructure status
make env status

# Verify services are running
docker ps
```

### 4. **Run the Complete Pipeline**
```bash
# Run the full data processing pipeline
make pipeline full
```

## 🧪 Testing

### **Run All Tests**
```bash
# Run complete test suite
make test all
```

### **Run Specific Test Suites**
```bash
# Show available test types
make test

# Unit tests only
make test unit

# Stream service tests
make test stream

# Batch service tests
make test batch

# Integration tests
make test integration
```

## 🔧 Development Commands

### **Pipeline Commands**
```bash
# Show available pipeline types
make pipeline

# Run batch processing
make pipeline batch

# Run streaming processing (2-terminal setup)
make stream produce    # Terminal 1
make stream assemble   # Terminal 2

# Or automated streaming
make pipeline stream

# Run complete pipeline
make pipeline full

# Use custom year/month
YEAR=2025 MONTH=02 make pipeline batch
```

### **Quality Commands**
```bash
# Show available lint operations
make lint

# Run pre-commit hooks
make lint check

# Auto-fix issues
make lint fix

# Update pre-commit hooks
make lint update
```

### **Environment Commands**
```bash
# Start infrastructure
make env up

# Stop infrastructure
make env down

# Check status
make env status

# View logs
make env logs

# Clean everything
make env clean
```

## 📊 Monitoring

### **Check Service Status**
```bash
# Check infrastructure status
make env status

# View running containers
docker ps

# Check service logs
docker logs <container-name>

# View all logs
make env logs
```

### **Database Access**
```bash
# Connect to PostgreSQL (using new command)
make db shell

# Or manually
docker exec -it <postgres-container> psql -U postgres -d chillflow

# View tables
\dt

# Check data
SELECT COUNT(*) FROM stg.complete_trip;
```

## 🚀 Production Deployment

### **Infrastructure as Code**
```bash
# Deploy infrastructure
cd infra/terraform
terraform init
terraform plan
terraform apply
```

### **Container Deployment**
```bash
# Build containers
docker build -t chillflow-batch ./backend/chillflow-batch
docker build -t chillflow-stream ./backend/chillflow-stream

# Deploy with Docker Compose
docker-compose -f platform/compose/docker-compose.yml up -d
```

## 🔍 Troubleshooting

### **Common Issues**

#### **Services Not Starting**
```bash
# Check Docker status
docker ps -a

# Check infrastructure status
make env status

# Restart services
make env down
make env up
```

#### **Database Connection Issues**
```bash
# Check PostgreSQL logs
docker logs <postgres-container>

# Verify connection
docker exec -it <postgres-container> psql -U postgres -d chillflow
```

#### **Kafka Connection Issues**
```bash
# Check Kafka logs
docker logs <kafka-container>

# Verify Kafka topics
docker exec -it <kafka-container> kafka-topics --list --bootstrap-server localhost:9092
```

### **Test Failures**
```bash
# Run tests with verbose output
make test all -v

# Run specific failing test
pytest tests/path/to/test.py -v

# Check test logs
pytest tests/ --tb=long
```

## 📚 Next Steps

1. **Explore the Codebase**: Check out the `backend/` directory
2. **Run Tests**: Ensure all tests pass
3. **Try the Pipeline**: Process some sample data
4. **Read Documentation**: Check out the architecture docs
5. **Contribute**: Make improvements and submit PRs

## 🆘 Getting Help

- **Documentation**: Check the `docs/` directory
- **Issues**: Create GitHub issues for bugs
- **Discussions**: Use GitHub discussions for questions
- **Code Review**: Submit PRs for improvements

## 🎯 Success Criteria

You'll know the setup is working when:
- ✅ All services start without errors
- ✅ All tests pass (`make test all`)
- ✅ Pipeline runs successfully (`make pipeline full`)
- ✅ Data appears in the database
- ✅ No error messages in logs
