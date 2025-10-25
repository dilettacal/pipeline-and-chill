# ChillFlow Platform - Development Makefile

.PHONY: help up down clean logs status setup-env test pipeline-ingestion pipeline-batch pipeline-stream pipeline-analytics pipeline-full

help: ## Show this help message
	@echo "ChillFlow Platform Development Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start infrastructure (usage: make up PROFILE=basic|observability)
	@if [ "$(PROFILE)" = "observability" ]; then \
		echo "🚀 Starting ChillFlow infrastructure with monitoring..."; \
		echo "📈 Grafana: http://localhost:3000 (admin/admin)"; \
		echo "📊 Prometheus: http://localhost:9090"; \
		PROFILE_FLAG="--profile observability"; \
	else \
		echo "🚀 Starting ChillFlow infrastructure..."; \
		PROFILE_FLAG=""; \
	fi; \
	if [ ! -f platform/compose/.env ]; then \
		echo "📝 Creating .env file from template..."; \
		cp platform/compose/env.example platform/compose/.env; \
	fi; \
	cd platform/compose && docker-compose $$PROFILE_FLAG up -d postgres redis zookeeper kafka; \
	echo "✅ Infrastructure ready!"; \
	echo "   📊 Database: postgresql://dev:dev@localhost:5432/chillflow"; \
	echo "   🔴 Redis: redis://localhost:6379/0"; \
	echo "   📨 Kafka: localhost:9092"

down: ## Stop all infrastructure
	@echo "🛑 Stopping ChillFlow infrastructure..."
	cd platform/compose && docker-compose down
	@echo "✅ Infrastructure stopped"

clean: ## Clean infrastructure and data (usage: make clean TYPE=all|db|containers)
	@if [ "$(TYPE)" = "db" ]; then \
		echo "🗑️  Clearing database tables..."; \
		uv run python scripts/data-management/clean_database.py --confirm; \
		echo "✅ Database cleanup complete!"; \
	elif [ "$(TYPE)" = "containers" ]; then \
		echo "🧹 Cleaning up containers and volumes..."; \
		cd platform/compose && docker-compose down -v --remove-orphans; \
		docker system prune -f; \
		echo "✅ Container cleanup complete!"; \
	else \
		echo "🧹 Cleaning up everything..."; \
		cd platform/compose && docker-compose down -v --remove-orphans; \
		docker system prune -f; \
		echo "✅ Full cleanup complete!"; \
	fi

logs: ## Show infrastructure logs
	cd platform/compose && docker-compose logs -f

status: ## Show infrastructure status
	@echo "📊 ChillFlow Infrastructure Status:"
	@echo ""
	@echo "🐘 PostgreSQL:"
	@docker exec chillflow-postgres pg_isready -U dev -d chillflow 2>/dev/null && echo "  ✅ Running" || echo "  ❌ Not running"
	@echo ""
	@echo "🔴 Redis:"
	@docker exec chillflow-redis redis-cli ping 2>/dev/null | grep -q PONG && echo "  ✅ Running" || echo "  ❌ Not running"
	@echo ""
	@echo "📨 Kafka:"
	@docker exec chillflow-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"
	@echo ""
	@echo "📈 Grafana:"
	@curl -s http://localhost:3000/api/health >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"
	@echo ""
	@echo "📊 Prometheus:"
	@curl -s http://localhost:9090/-/healthy >/dev/null 2>&1 && echo "  ✅ Running" || echo "  ❌ Not running"

test: ## Run tests (usage: make test TYPE=unit|infra|stream|stream-integration|all)
	@if [ "$(TYPE)" = "infra" ]; then \
		echo "🧪 Running infrastructure tests..."; \
		echo "⚠️  Make sure infrastructure is running: make up"; \
		uv run pytest tests/infrastructure/ -m infrastructure -v; \
	elif [ "$(TYPE)" = "stream" ]; then \
		echo "🧪 Running stream service unit tests..."; \
		cd backend/chillflow-stream && uv run pytest tests/ -m "not integration" -v; \
	elif [ "$(TYPE)" = "stream-integration" ]; then \
		echo "🧪 Running stream service integration tests..."; \
		echo "⚠️  This requires Docker and testcontainers"; \
		cd backend/chillflow-stream && uv run pytest tests/ -m integration -v; \
	elif [ "$(TYPE)" = "all" ]; then \
		echo "🧪 Running all tests..."; \
		uv run pytest tests/ -v; \
		echo "🧪 Running stream service tests..."; \
		cd backend/chillflow-stream && uv run pytest tests/ -v; \
	else \
		echo "🧪 Running unit tests..."; \
		uv run pytest tests/unit/ -v; \
	fi


pipeline-ingestion: ## Run data ingestion pipeline
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

pipeline-batch: ## Run batch processing pipeline
	@echo "🔄 Running batch processing pipeline..."
	@echo "  1. Processing curated trip data..."
	@uv run python -m batch process trips data/curated/yellow/2025/01/yellow_tripdata_2025-01.parquet
	@echo "  2. Computing KPIs..."
	@uv run python -m batch aggregate run
	@echo "✅ Batch processing complete!"

pipeline-stream: ## Run streaming pipeline
	@echo "🌊 Running streaming pipeline..."
	@echo "  1. Starting replay producer..."
	@uv run python -m stream replay start
	@echo "  2. Starting trip assembler..."
	@uv run python -m stream assembler start
	@echo "✅ Streaming pipeline active!"

pipeline-analytics: ## Run analytics pipeline
	@echo "📊 Running analytics pipeline..."
	@echo "  1. Computing zone hourly KPIs..."
	@uv run python -m batch aggregate run
	@echo "  2. Generating reports..."
	@uv run python scripts/analytics/generate_reports.py
	@echo "✅ Analytics complete!"

pipeline-full: ## Run complete end-to-end pipeline
	@echo "🚀 Running complete data pipeline..."
	@echo "  📥 Ingestion: Download, seed zones, curate data"
	@if [ -f scripts/data-management/download_nyc_data.sh ]; then \
		chmod +x scripts/data-management/download_nyc_data.sh; \
		./scripts/data-management/download_nyc_data.sh; \
	fi
	@uv run python backend/chillflow-core/core/migrations/seed_zones.py
	@uv run python scripts/data-management/curate_all_data.py --data-root data
	@echo "  🔄 Batch: Process trips, compute KPIs"
	@uv run python -m batch process trips $(YEAR) $(MONTH)
	@uv run python -m batch aggregate run
	@echo "  🌊 Stream: Start trip event producer and assembler"
	@uv run python -m stream.cli produce-events --limit 100 &
	@uv run python -m stream.cli assemble-trips --timeout 30 &
	@echo "  📊 Analytics: Generate reports"
	@uv run python scripts/analytics/generate_reports.py
	@echo "✅ Complete pipeline finished!"

setup-env: ## Create .env file from template
	@echo "📝 Setting up environment file..."
	@if [ ! -f platform/compose/.env ]; then \
		cp platform/compose/env.example platform/compose/.env; \
		echo "✅ Created platform/compose/.env from template"; \
		echo "📝 Edit platform/compose/.env to customize your setup"; \
	else \
		echo "⚠️  .env file already exists at platform/compose/.env"; \
		echo "📝 Edit it manually if you need to change settings"; \
	fi

# Code quality and linting
lint: ## Run pre-commit hooks on all files
	@echo "🔍 Running pre-commit hooks on all files..."
	uv run pre-commit run --all-files

lint-fix: ## Run pre-commit hooks and auto-fix issues
	@echo "🔧 Running pre-commit hooks with auto-fix..."
	uv run pre-commit run --all-files --hook-stage manual

lint-update: ## Update pre-commit hooks to latest versions
	@echo "🔄 Updating pre-commit hooks..."
	uv run pre-commit autoupdate

# Stream processing pipeline
pipeline-stream: ## Run streaming pipeline (produce events + assemble trips)
	@echo "🌊 Running streaming pipeline..."
	@echo "  1. Creating Kafka topic..."
	@uv run python -m stream.cli create-topic --topic trip-events
	@echo "  2. Producing events from database..."
	@uv run python -m stream.cli produce-events --limit 50 --topic trip-events
	@echo "  3. Assembling events into trips..."
	@uv run python -m stream.cli assemble-trips --topic trip-events --timeout 10
	@echo "✅ Streaming pipeline complete!"

stream-produce: ## Produce trip events from database
	@echo "📡 Producing trip events..."
	@uv run python -m stream.cli produce-events --limit 100

stream-assemble: ## Assemble events into complete trips
	@echo "🔧 Assembling events into trips..."
	@uv run python -m stream.cli assemble-trips --timeout 30

stream-consume: ## Consume and display events from Kafka
	@echo "👀 Consuming events from Kafka..."
	@uv run python -m stream.cli consume-events --topic trip-events --timeout 10

stream-create-topic: ## Create Kafka topic for trip events
	@echo "📋 Creating Kafka topic..."
	@uv run python -m stream.cli create-topic --topic trip-events
