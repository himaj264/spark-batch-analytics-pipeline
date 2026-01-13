# =====================================================
# Spark Batch Analytics Pipeline - Makefile
# =====================================================
# Common commands for development and deployment

.PHONY: help build run stop clean logs test query

# Default target
help:
	@echo "Spark Batch Analytics Pipeline - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "  make build    - Build Docker images"
	@echo "  make run      - Run the complete pipeline"
	@echo "  make stop     - Stop all containers"
	@echo "  make clean    - Remove containers, images, and volumes"
	@echo "  make logs     - View Spark ETL logs"
	@echo "  make test     - Run unit tests"
	@echo "  make query    - Connect to PostgreSQL"
	@echo "  make status   - Check container status"
	@echo ""

# Build Docker images
build:
	@echo "Building Docker images..."
	docker-compose build

# Run the complete pipeline
run:
	@echo "Starting pipeline..."
	docker-compose up

# Run in detached mode
run-detached:
	@echo "Starting pipeline in background..."
	docker-compose up -d

# Stop all containers
stop:
	@echo "Stopping containers..."
	docker-compose down

# Clean everything
clean:
	@echo "Cleaning up..."
	docker-compose down -v --rmi local
	rm -rf data/processed/*
	@echo "Cleanup complete!"

# View logs
logs:
	docker-compose logs spark-etl

# Follow logs in real-time
logs-follow:
	docker-compose logs -f spark-etl

# Run tests
test:
	docker-compose run --rm spark-etl pytest /app/tests/ -v

# Connect to PostgreSQL
query:
	@echo "Connecting to PostgreSQL..."
	docker exec -it analytics-postgres psql -U analytics_user -d analytics_db

# Check container status
status:
	docker-compose ps

# Rebuild and run
rebuild: clean build run
