# ═══════════════════════════════════════════════════════
#   FoodStream Analytics — Makefile
#   Convenience commands for managing the pipeline
# ═══════════════════════════════════════════════════════

.PHONY: start stop restart logs producer-logs spark-logs pinot-logs query status clean help

## ── Lifecycle ────────────────────────────────────────
start:         ## Start all services in background
	@echo "🚀 Starting FoodStream Analytics pipeline..."
	docker-compose up -d
	@echo ""
	@echo "╔══════════════════════════════════════════════════╗"
	@echo "║   Services are starting up. Access points:       ║"
	@echo "║                                                  ║"
	@echo "║   🔥 Kafka UI   →  http://localhost:8888         ║"
	@echo "║   🔍 Pinot UI   →  http://localhost:9000         ║"
	@echo "║   📡 Pinot API  →  http://localhost:8099         ║"
	@echo "║   📊 Kafka      →  localhost:29092 (external)    ║"
	@echo "╚══════════════════════════════════════════════════╝"
	@echo ""
	@echo "💡 Run 'make logs' to watch all service logs."

stop:          ## Stop all services
	docker-compose down
	@echo "🛑 All services stopped."

restart:       ## Restart all services
	docker-compose down && docker-compose up -d

clean:         ## Stop services and remove all volumes (full reset)
	@echo "⚠️  This will remove all data! Are you sure? (Ctrl+C to cancel)"
	@sleep 3
	docker-compose down -v --remove-orphans
	@echo "🧹 Cleaned up all containers and volumes."

## ── Logs ─────────────────────────────────────────────
logs:          ## Tail logs of all services
	docker-compose logs -f

producer-logs: ## Tail the food order producer logs
	docker-compose logs -f producer

spark-logs:    ## Tail the PySpark processor logs
	docker-compose logs -f spark-processor

pinot-logs:    ## Tail all Pinot service logs
	docker-compose logs -f pinot-controller pinot-broker pinot-server pinot-init

## ── Query & Status ───────────────────────────────────
query:         ## Run example Pinot SQL queries
	@bash scripts/test_queries.sh

status:        ## Show running container status
	docker-compose ps

## ── Help ─────────────────────────────────────────────
help:          ## Show this help
	@echo ""
	@echo "FoodStream Analytics — Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
