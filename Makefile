# ChillFlow Platform - Development Makefile

.PHONY: help up down clean clean-db logs status setup-env test curate-all download-data

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

clean: ## Remove all containers and volumes
	@echo "🧹 Cleaning up ChillFlow infrastructure..."
	cd platform/compose && docker-compose down -v --remove-orphans
	docker system prune -f
	@echo "✅ Cleanup complete"

clean-db: ## Clear database tables (keeps infrastructure running)
	echo "🗑️  Clearing database tables..."
	uv run python scripts/data-management/clean_database.py --confirm
	@echo "✅ Database cleanup complete!"

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

test: ## Run tests (usage: make test TYPE=unit|infra|all)
	@if [ "$(TYPE)" = "infra" ]; then \
		echo "🧪 Running infrastructure tests..."; \
		echo "⚠️  Make sure infrastructure is running: make up"; \
		uv run pytest tests/infrastructure/ -m infrastructure -v; \
	elif [ "$(TYPE)" = "all" ]; then \
		echo "🧪 Running all tests..."; \
		uv run pytest tests/ -v; \
	else \
		echo "🧪 Running unit tests..."; \
		uv run pytest tests/unit/ -v; \
	fi

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

# ========================================================================
# Data Management Commands
# ========================================================================

download-data: ## Download NYC taxi data, reference files, and seed zones
	@echo "📥 Downloading NYC taxi data and reference files..."
	@if [ -f scripts/data-management/download_nyc_data.sh ]; then \
		chmod +x scripts/data-management/download_nyc_data.sh; \
		./scripts/data-management/download_nyc_data.sh; \
	else \
		echo "❌ Download script not found. Please ensure scripts/data-management/download_nyc_data.sh exists"; \
		exit 1; \
	fi
	@echo "🌍 Seeding taxi zones into database..."
	@echo "⚠️  Make sure infrastructure is running: make up"
	uv run python backend/chillflow-core/core/migrations/seed_zones.py
	@echo "✅ Data download and zone seeding complete!"

curate-all: ## Curate all available raw data (dynamically discovered)
	@echo "🔄 Curating all available raw data..."
	@if [ ! -d "data/raw/yellow" ]; then \
		echo "❌ No raw data found. Download data first: make download-data"; \
		exit 1; \
	fi
	uv run python scripts/data-management/curate_all_data.py --data-root data
	@echo "🎉 All data curation complete!"
	@echo "📊 Curated data available in: data/curated/yellow/"
