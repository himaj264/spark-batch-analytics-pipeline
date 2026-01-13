#!/bin/bash
# =====================================================
# Spark Batch Analytics Pipeline - Run Script
# =====================================================
# This script builds and runs the complete pipeline

set -e  # Exit on error

echo "=========================================="
echo "Spark Batch Analytics Pipeline"
echo "=========================================="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker Desktop."
    exit 1
fi

print_status "Docker is running"

# Navigate to project directory
cd "$(dirname "$0")/.."

# Step 1: Build Docker images
print_status "Building Docker images..."
docker-compose build --no-cache

# Step 2: Start PostgreSQL first
print_status "Starting PostgreSQL database..."
docker-compose up -d postgres

# Wait for PostgreSQL to be healthy
print_status "Waiting for PostgreSQL to be ready..."
sleep 10

# Step 3: Run the Spark ETL pipeline
print_status "Running Spark ETL Pipeline..."
docker-compose up spark-etl

# Step 4: Show results
print_status "Pipeline completed!"
echo ""
echo "=========================================="
echo "Results Summary"
echo "=========================================="
echo ""
echo "Processed data saved to: ./data/processed/"
echo ""
echo "To query PostgreSQL results:"
echo "  docker exec -it analytics-postgres psql -U analytics_user -d analytics_db"
echo ""
echo "Sample queries:"
echo "  SELECT * FROM agg_sales_by_category;"
echo "  SELECT * FROM agg_sales_by_region;"
echo "  SELECT * FROM agg_customer_summary LIMIT 10;"
echo ""
echo "=========================================="

# Optional: Keep containers running or stop
read -p "Keep PostgreSQL running? (y/n): " keep_running
if [ "$keep_running" != "y" ]; then
    print_status "Stopping all containers..."
    docker-compose down
fi

print_status "Done!"
