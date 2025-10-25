# ChillFlow Platform - Development Makefile

# Project root directory
PROJECT_ROOT := $(shell pwd)

# Pipeline configuration
YEAR ?= 2025
MONTH ?= 01
CURATED_PATH := data/curated/yellow/$(YEAR)/$(MONTH)/yellow_tripdata_$(YEAR)-$(MONTH).parquet

.PHONY: help env db test pipeline pipeline-ingestion pipeline-batch pipeline-stream pipeline-analytics pipeline-full redis stream unit infra stream-integration batch batch-integration contracts smoke performance all produce assemble consume topic produce-continuous redis redis-continuous lint check fix update start stop logs cli test up up-observability down clean setup db-clean db-shell shell observability frontend

help: ## Show this help message
	@echo "ChillFlow Platform Development Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

env: ## Environment operations (usage: make env <operation>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		PROFILE_FLAG=""; \
		if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up-observability" ]; then \
			echo "🚀 Starting ChillFlow infrastructure with monitoring..."; \
			echo "📈 Grafana: http://localhost:3000 (admin/admin)"; \
			echo "📊 Prometheus: http://localhost:9090"; \
			PROFILE_FLAG="--profile observability"; \
		else \
			echo "🚀 Starting ChillFlow infrastructure..."; \
		fi; \
		if [ ! -f platform/compose/.env ]; then \
			echo "📝 Creating .env file from template..."; \
			cp platform/compose/env.example platform/compose/.env; \
		fi; \
		cd platform/compose && docker-compose $$PROFILE_FLAG up -d postgres redis zookeeper kafka >/dev/null 2>&1; \
		echo "⏳ Waiting for services to be ready..."; \
		sleep 5; \
		echo "🔧 Setting up database schema..."; \
		cd $(PROJECT_ROOT) && uv run python backend/chillflow-core/core/migrations/setup_database.py >/dev/null 2>&1; \
		echo "✅ Infrastructure ready!"; \
		echo "   📊 Database: postgresql://dev:dev@localhost:5432/chillflow"; \
		echo "   🔴 Redis: redis://localhost:6379/0"; \
		echo "   📨 Kafka: localhost:9092"; \
		echo "   🐘 Zookeeper: localhost:2181"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		echo "🛑 Stopping ChillFlow infrastructure..."; \
		cd platform/compose && docker-compose down >/dev/null 2>&1; \
		echo "✅ Infrastructure stopped"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "clean" ]; then \
		echo "🧹 Cleaning up everything..."; \
		cd platform/compose && docker-compose down -v --remove-orphans; \
		docker system prune -f; \
		echo "✅ Full cleanup complete!"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "logs" ]; then \
		cd platform/compose && docker-compose logs -f; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "status" ]; then \
		echo "📊 ChillFlow Infrastructure Status:"; \
		echo ""; \
		echo "🐘 PostgreSQL:"; \
		docker exec chillflow-postgres pg_isready -U dev -d chillflow 2>/dev/null && echo "  ✅ Running" || echo "  ❌ Not running"; \
		echo ""; \
		echo "🔴 Redis:"; \
		docker exec chillflow-redis redis-cli ping 2>/dev/null | grep -q PONG && echo "  ✅ Running" || echo "  ❌ Not running"; \
		echo ""; \
		echo "📨 Kafka:"; \
		docker exec chillflow-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"; \
		echo ""; \
		echo "📈 Grafana:"; \
		curl -s http://localhost:3000/api/health >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"; \
		echo ""; \
		echo "📊 Prometheus:"; \
		curl -s http://localhost:9090/-/healthy >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "setup" ]; then \
		echo "📝 Setting up environment file..."; \
		if [ ! -f platform/compose/.env ]; then \
			cp platform/compose/env.example platform/compose/.env; \
			echo "✅ Created platform/compose/.env from template"; \
			echo "📝 Edit platform/compose/.env to customize your setup"; \
		else \
			echo "⚠️  .env file already exists at platform/compose/.env"; \
			echo "📝 Edit it manually if you need to change settings"; \
		fi; \
	else \
		echo "🌍 You have not specified an environment operation. Available operations:"; \
		echo "   make env up              - Start infrastructure"; \
		echo "   make env up-observability - Start infrastructure with monitoring"; \
		echo "   make env down            - Stop all infrastructure"; \
		echo "   make env clean           - Clean everything"; \
		echo "   make env logs            - Show infrastructure logs"; \
		echo "   make env status         - Show infrastructure status"; \
		echo "   make env setup           - Create .env file from template"; \
		echo ""; \
		echo "🌍 Please specify an environment operation to run."; \
	fi

# Database operations
db: ## Database operations (usage: make db <operation>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "clean" ]; then \
		echo "🗑️  Clearing database tables..."; \
		if uv run python scripts/data-management/clean_database.py --confirm; then \
			echo "✅ Database cleanup complete!"; \
		else \
			echo "❌ Database cleanup failed!"; \
		fi; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "shell" ]; then \
		echo "🐘 Connecting to PostgreSQL shell..."; \
		echo "   Type \\q to exit the shell"; \
		docker exec -it chillflow-postgres psql -U dev -d chillflow; \
	else \
		echo "🗄️  You have not specified a database operation. Available operations:"; \
		echo "   make db clean  - Clear database tables"; \
		echo "   make db shell  - Connect to PostgreSQL shell"; \
		echo ""; \
		echo "📊 Example queries to run in the database shell:"; \
		echo "   \\dt stg.*                    - List staging tables"; \
		echo "   \\dt dim.*                    - List dimension tables"; \
		echo "   SELECT COUNT(*) FROM stg.complete_trip;  - Count trips"; \
		echo "   SELECT * FROM dim.zone LIMIT 5;           - Show zones"; \
		echo "   \\q                           - Exit shell"; \
		echo ""; \
		echo "🗄️  Please specify a database operation to run."; \
	fi

# Make the database operations available as targets
db-clean:
	@:

# Make the environment operations available as targets
up up-observability down clean logs status setup:
	@:

test: ## Run tests (usage: make test <test-type>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "unit" ]; then \
		echo "🧪 Running core unit tests..."; \
		uv run pytest tests/unit/ -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "infra" ]; then \
		echo "🧪 Running infrastructure tests..."; \
		echo "⚠️  Make sure infrastructure is running: make up"; \
		uv run pytest tests/infrastructure/ -m infrastructure -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stream" ]; then \
		echo "🧪 Running stream service unit tests..."; \
		cd $(PROJECT_ROOT)/backend/chillflow-stream && uv run pytest tests/unit/ -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stream-integration" ]; then \
		echo "🧪 Running stream service integration tests..."; \
		echo "⚠️  This requires Docker and testcontainers"; \
		cd $(PROJECT_ROOT)/backend/chillflow-stream && uv run pytest tests/integration/ -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "batch" ]; then \
		echo "🧪 Running batch service unit tests..."; \
		cd $(PROJECT_ROOT)/backend/chillflow-batch && uv run pytest tests/ -m "not integration" -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "batch-integration" ]; then \
		echo "🧪 Running batch service integration tests..."; \
		echo "⚠️  This requires Docker and testcontainers"; \
		cd $(PROJECT_ROOT)/backend/chillflow-batch && uv run pytest tests/test_simple_integration.py -m integration -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "contracts" ]; then \
		echo "🧪 Running contract tests..."; \
		uv run pytest tests/contracts/ -m contract -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "smoke" ]; then \
		echo "🧪 Running smoke tests..."; \
		uv run pytest tests/smoke/ -m smoke -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "performance" ]; then \
		echo "🧪 Running performance tests..."; \
		uv run pytest tests/performance/ -m performance -v; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "all" ]; then \
		echo "🧪 Running all unit tests..."; \
		uv run pytest tests/unit/ -v; \
		echo "🧪 Running stream service unit tests..."; \
		cd $(PROJECT_ROOT)/backend/chillflow-stream && uv run pytest tests/unit/ -v; \
		echo "🧪 Running batch service unit tests..."; \
		cd $(PROJECT_ROOT)/backend/chillflow-batch && uv run pytest tests/ -m "not integration" -v; \
		echo "🧪 Running stream integration tests..."; \
		cd $(PROJECT_ROOT)/backend/chillflow-stream && uv run pytest tests/integration/ -v; \
		echo "🧪 Running batch integration tests..."; \
		cd $(PROJECT_ROOT)/backend/chillflow-batch && uv run pytest tests/ -m integration -v; \
		echo "🧪 Running contract tests..."; \
		cd $(PROJECT_ROOT) && uv run pytest tests/contracts/ -m contract -v; \
		echo "🧪 Running smoke tests..."; \
		cd $(PROJECT_ROOT) && uv run pytest tests/smoke/ -m smoke -v; \
	else \
		echo "🧪 You have not specified a test type. Available test types:"; \
		echo "   make test unit              - Run core unit tests"; \
		echo "   make test infra             - Run infrastructure tests"; \
		echo "   make test stream             - Run stream service unit tests"; \
		echo "   make test stream-integration - Run stream service integration tests"; \
		echo "   make test batch             - Run batch service unit tests"; \
		echo "   make test batch-integration  - Run batch service integration tests"; \
		echo "   make test contracts         - Run contract tests"; \
		echo "   make test smoke             - Run smoke tests"; \
		echo "   make test performance       - Run performance tests"; \
		echo "   make test all               - Run all tests"; \
		echo ""; \
		echo "🧪 Please specify a test type to run tests."; \
	fi

# Make the test types available as targets
unit infra stream-integration batch batch-integration contracts smoke performance all:
	@:

# Make the pipeline types available as targets
ingestion analytics full:
	@:


pipeline: ## Pipeline operations (usage: make pipeline <type>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "ingestion" ]; then \
		echo "📥 Running data ingestion pipeline..."; \
		$(MAKE) pipeline-ingestion; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "batch" ]; then \
		echo "🔄 Running batch processing pipeline..."; \
		$(MAKE) pipeline-batch; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stream" ]; then \
		echo "🌊 Running streaming pipeline..."; \
		$(MAKE) pipeline-stream; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "analytics" ]; then \
		echo "📊 Running analytics pipeline..."; \
		$(MAKE) pipeline-analytics; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "full" ]; then \
		echo "🚀 Running complete pipeline..."; \
		$(MAKE) pipeline-full; \
	else \
		echo "🚀 You have not specified a pipeline type. Available pipeline types:"; \
		echo "   make pipeline ingestion  - Run data ingestion pipeline"; \
		echo "   make pipeline batch      - Run batch processing pipeline"; \
		echo "   make pipeline stream     - Run streaming pipeline"; \
		echo "   make pipeline analytics  - Run analytics pipeline"; \
		echo "   make pipeline full       - Run complete end-to-end pipeline"; \
		echo ""; \
		echo "💡 Make sure infrastructure is running first:"; \
		echo "   make env up              - Start PostgreSQL, Redis, Kafka"; \
		echo "   make env status          - Check infrastructure status"; \
		echo ""; \
		echo "🔧 Pipeline configuration (optional):"; \
		echo "   YEAR=2025 MONTH=01 make pipeline batch  - Use specific year/month"; \
		echo "   Default: YEAR=2025 MONTH=01"; \
		echo ""; \
		echo "🌊 Streaming pipeline requires 2 terminals:"; \
		echo "   Terminal 1: make stream produce    - Start event producer"; \
		echo "   Terminal 2: make stream assemble   - Start trip assembler"; \
		echo "   Or use: make pipeline stream       - Automated streaming"; \
		echo ""; \
		echo "🚀 Please specify a pipeline type to run."; \
	fi

# Individual pipeline commands
pipeline-ingestion:
	@echo "📥 Running data ingestion pipeline..."
	@echo "  1. Running database migrations..."
	@cd backend/chillflow-core/core/migrations && uv run alembic upgrade head
	@echo "  2. Downloading NYC taxi data and reference files..."
	@if [ -f scripts/data-management/download_nyc_data.sh ]; then \
		chmod +x scripts/data-management/download_nyc_data.sh; \
		./scripts/data-management/download_nyc_data.sh; \
	else \
		echo "❌ Download script not found"; \
		exit 1; \
	fi
	@echo "  3. Seeding taxi zones into database..."
	@uv run python backend/chillflow-core/core/migrations/seed_zones.py
	@echo "  4. Curating raw data..."
	@uv run python scripts/data-management/curate_all_data.py --data-root data
	@echo "✅ Ingestion complete!"

pipeline-batch:
	@echo "🔄 Running batch processing pipeline..."
	@echo "  1. Processing curated trip data..."
	@uv run python -m batch process trips $(CURATED_PATH)
	@echo "  2. Computing KPIs..."
	@uv run python -m batch aggregate run
	@echo "✅ Batch processing complete!"

pipeline-stream:
	@echo "🌊 Running streaming pipeline..."
	@echo "  1. Creating Kafka topic..."
	@uv run python -m stream.cli create-topic --topic trip-events
	@echo "  2. Producing events from database..."
	@uv run python -m stream.cli produce-events --limit 50 --topic trip-events
	@echo "  3. Assembling events into trips..."
	@uv run python -m stream.cli assemble-trips --topic trip-events --timeout 10
	@echo "✅ Streaming pipeline complete!"

pipeline-analytics:
	@echo "📊 Running analytics pipeline..."
	@echo "  1. Computing zone hourly KPIs..."
	@uv run python -m batch aggregate run
	@echo "  2. Generating reports..."
	@uv run python scripts/analytics/generate_reports.py
	@echo "✅ Analytics complete!"

pipeline-full:
	@echo "🚀 Running complete data pipeline..."
	@echo "  📥 Ingestion: Download, seed zones, curate data"
	@if [ -f scripts/data-management/download_nyc_data.sh ]; then \
		chmod +x scripts/data-management/download_nyc_data.sh; \
		./scripts/data-management/download_nyc_data.sh; \
	fi
	@uv run python backend/chillflow-core/core/migrations/seed_zones.py
	@uv run python scripts/data-management/curate_all_data.py --data-root data
	@echo "  🔄 Batch: Process trips, compute KPIs"
	@uv run python -m batch process trips $(CURATED_PATH)
	@uv run python -m batch aggregate run
	@echo "  🌊 Stream: Start trip event producer and assembler"
	@uv run python -m stream.cli produce-events --limit 100 &
	@uv run python -m stream.cli assemble-trips --timeout 30 &
	@echo "  📊 Analytics: Generate reports"
	@uv run python scripts/analytics/generate_reports.py
	@echo "✅ Complete pipeline finished!"


# Code quality and linting
lint: ## Lint operations (usage: make lint <operation>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "check" ]; then \
		echo "🔍 Running pre-commit hooks on all files..."; \
		uv run pre-commit run --all-files; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "fix" ]; then \
		echo "🔧 Running pre-commit hooks with auto-fix..."; \
		uv run pre-commit run --all-files --hook-stage manual; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "update" ]; then \
		echo "🔄 Updating pre-commit hooks..."; \
		uv run pre-commit autoupdate; \
	else \
		echo "🔍 You have not specified a lint operation. Available operations:"; \
		echo "   make lint check   - Run pre-commit hooks on all files"; \
		echo "   make lint fix     - Run pre-commit hooks and auto-fix issues"; \
		echo "   make lint update  - Update pre-commit hooks to latest versions"; \
		echo ""; \
		echo "🔍 Running lint check by default..."; \
		uv run pre-commit run --all-files; \
	fi

# Make the lint operations available as targets
check fix update:
	@:


stream: ## Stream operations (usage: make stream <operation>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "produce" ]; then \
		echo "📡 Producing trip events..."; \
		uv run python -m stream.cli produce-events --limit 100; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "assemble" ]; then \
		echo "🔧 Assembling events into trips..."; \
		uv run python -m stream.cli assemble-trips --timeout 30; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "consume" ]; then \
		echo "👀 Consuming events from Kafka..."; \
		uv run python -m stream.cli consume-events --topic trip-events --timeout 10; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "topic" ]; then \
		echo "📋 Creating Kafka topic..."; \
		uv run python -m stream.cli create-topic --topic trip-events; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "produce-continuous" ]; then \
		echo "📡 Producing events continuously..."; \
		echo "   📋 Ensuring Kafka topic exists..."; \
		uv run python -m stream.cli create-topic --topic trip-events || echo "   ℹ️  Topic may already exist (this is OK)"; \
		echo "   📡 Starting continuous event production (1000 events per batch)..."; \
		while true; do \
			echo "   📡 Producing batch of events..."; \
			uv run python -m stream.cli produce-events --limit 1000 --topic trip-events; \
			echo "   ⏳ Waiting 5 seconds before next batch..."; \
			sleep 5; \
		done; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "assemble-redis" ]; then \
		TIMEOUT=$${2:-10}; \
		echo "🔧 Assembling events with Redis state management (timeout: $$TIMEOUT seconds)..."; \
		echo "   📋 Ensuring Kafka topic exists..."; \
		uv run python -m stream.cli create-topic --topic trip-events || echo "   ℹ️  Topic may already exist (this is OK)"; \
		echo "   🔧 Assembling events with Redis..."; \
		uv run python -m stream.cli assemble-trips --use-redis --redis-url redis://localhost:6379/0 --timeout $$TIMEOUT; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "assemble-redis-continuous" ]; then \
		TIMEOUT=$${2:-30}; \
		echo "🔧 Assembling events with Redis state management (continuous, timeout: $$TIMEOUT seconds per batch)..."; \
		echo "   🔧 Starting continuous assembly with Redis..."; \
		uv run python -m stream.cli assemble-trips --use-redis --redis-url redis://localhost:6379/0 --timeout $$TIMEOUT; \
	else \
		echo "🌊 You have not specified a stream operation. Available operations:"; \
		echo "   make stream produce              - Produce trip events from database"; \
		echo "   make stream assemble            - Assemble events into complete trips"; \
		echo "   make stream consume              - Consume and display events from Kafka"; \
		echo "   make stream topic                - Create Kafka topic for trip events"; \
		echo "   make stream produce-continuous   - Produce events continuously (run in Terminal 1)"; \
		echo "   make stream assemble-redis [timeout]       - Assemble events with Redis (default: 10s)"; \
		echo "   make stream assemble-redis-continuous [timeout] - Assemble events with Redis continuously (default: 30s)"; \
	fi

# Make the stream operations available as targets
produce assemble consume topic produce-continuous assemble-redis assemble-redis-continuous:
	@:

# Redis management
redis: ## Redis operations (usage: make redis <operation>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "start" ]; then \
		echo "🔴 Starting Redis..."; \
		cd platform/compose && docker-compose up -d redis; \
		echo "✅ Redis started at redis://localhost:6379/0"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stop" ]; then \
		echo "🛑 Stopping Redis..."; \
		cd platform/compose && docker-compose stop redis; \
		echo "✅ Redis stopped"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "logs" ]; then \
		echo "📋 Redis logs:"; \
		cd platform/compose && docker-compose logs -f redis; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "cli" ]; then \
		echo "🔧 Connecting to Redis CLI..."; \
		docker exec -it chillflow-redis redis-cli; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test" ]; then \
		echo "🧪 Testing Redis connection..."; \
		uv run python -c "import redis; r = redis.from_url('redis://localhost:6379/0'); print('✅ Redis connected:', r.ping())"; \
	else \
		echo "🔴 You have not specified a Redis operation. Available operations:"; \
		echo "   make redis start  - Start Redis only"; \
		echo "   make redis stop   - Stop Redis only"; \
		echo "   make redis logs   - Show Redis logs"; \
		echo "   make redis cli    - Connect to Redis CLI"; \
		echo "   make redis test   - Test Redis connection"; \
	fi

# Make the Redis operations available as targets
start stop cli:
	@:

# Observability operations
observability: ## Observability operations (usage: make observability <operation>)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "up" ]; then \
		echo "🚀 Starting ChillFlow Observatory..."; \
		docker-compose -f platform/compose/docker-compose.yml -f platform/compose/docker-compose.observability.yml --profile observability up -d >/dev/null 2>&1; \
		echo "⏳ Waiting for monitoring services to be ready..."; \
		sleep 10; \
		echo "✅ Observatory started!"; \
		echo ""; \
		echo "📊 Access your dashboards:"; \
		echo "   Grafana:    http://localhost:3000 (admin/admin)"; \
		echo "   Prometheus: http://localhost:9090"; \
		echo "   Loki:       http://localhost:3100"; \
		echo ""; \
		echo "🔍 Dashboards available:"; \
		echo "   Infrastructure Health: http://localhost:3000/d/infrastructure-health"; \
		echo "   Live Logs:            http://localhost:3000/d/live-logs"; \
		echo "   Pipeline Metrics:     http://localhost:3000/d/pipeline-metrics"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "down" ]; then \
		echo "🛑 Stopping ChillFlow Observatory..."; \
		docker-compose -f platform/compose/docker-compose.yml -f platform/compose/docker-compose.observability.yml --profile observability down >/dev/null 2>&1; \
		echo "✅ Observatory stopped!"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "status" ]; then \
		echo "📊 ChillFlow Observatory Status:"; \
		echo ""; \
		echo "🔍 Grafana:"; \
		curl -s http://localhost:3000/api/health >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"; \
		echo ""; \
		echo "📈 Prometheus:"; \
		curl -s http://localhost:9090/-/healthy >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"; \
		echo ""; \
		echo "📋 Loki:"; \
		curl -s http://localhost:3100/ready >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"; \
		echo ""; \
		echo "📊 cAdvisor:"; \
		curl -s http://localhost:8080/healthz >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "logs" ]; then \
		echo "📋 Observatory logs:"; \
		docker-compose -f platform/compose/docker-compose.yml -f platform/compose/docker-compose.observability.yml --profile observability logs -f; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "metrics" ]; then \
		echo "📊 Starting metrics server..."; \
		cd $(PROJECT_ROOT)/backend/chillflow-stream && uv run python -m stream.cli metrics; \
	else \
		echo "🔍 You have not specified an observability operation. Available operations:"; \
		echo "   make observability up      - Start monitoring stack"; \
		echo "   make observability down   - Stop monitoring stack"; \
		echo "   make observability status - Check monitoring status"; \
		echo "   make observability logs    - Show monitoring logs"; \
		echo "   make observability metrics - Start metrics server"; \
		echo ""; \
		echo "🔍 Please specify an observability operation to run."; \
	fi

# Frontend commands
frontend:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "streamlit" ]; then \
		echo "🌊 Starting Streamlit Dashboard..."; \
		cd $(PROJECT_ROOT) && uv run streamlit run frontend/streamlit/dashboard.py --server.port 8501 --server.address 0.0.0.0; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "ai" ]; then \
		echo "🤖 Starting AI-Powered Dashboard..."; \
		cd $(PROJECT_ROOT) && uv run streamlit run frontend/streamlit/dashboard_ai.py --server.port 8502 --server.address 0.0.0.0; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "react" ]; then \
		echo "⚛️ Starting React Dashboard..."; \
		cd $(PROJECT_ROOT)/frontend/react && npx vite; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "install" ]; then \
		echo "📦 Installing frontend dependencies..."; \
		cd $(PROJECT_ROOT) && uv pip install streamlit plotly requests openai anthropic; \
		cd $(PROJECT_ROOT)/frontend/react && npm install; \
		echo "✅ Frontend dependencies installed!"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "build" ]; then \
		echo "🏗️ Building frontend..."; \
		cd $(PROJECT_ROOT)/frontend/react && npx vite build; \
		echo "✅ Frontend built!"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "dev" ]; then \
		echo "🚀 Starting development environment..."; \
		echo "   Streamlit: http://localhost:8501"; \
		echo "   React:     http://localhost:3000"; \
		echo "   Metrics:   http://localhost:8000"; \
		echo ""; \
		echo "💡 Run 'make observability metrics' in another terminal for metrics"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test" ]; then \
		echo "🧪 Running frontend tests..."; \
		cd $(PROJECT_ROOT)/frontend/react && npx vitest run; \
	else \
		echo "🎨 You have not specified a frontend operation. Available operations:"; \
		echo "   make frontend streamlit  - Start Streamlit dashboard"; \
		echo "   make frontend ai         - Start AI-powered dashboard"; \
		echo "   make frontend react      - Start React dashboard"; \
		echo "   make frontend install    - Install all dependencies"; \
		echo "   make frontend build      - Build for production"; \
		echo "   make frontend dev        - Start development environment"; \
		echo "   make frontend test       - Run frontend tests"; \
		echo ""; \
		echo "🎨 Please specify a frontend operation to run."; \
	fi

# Make the observability operations available as targets
metrics:
	@:
