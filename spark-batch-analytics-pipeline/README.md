# Spark Batch Analytics Pipeline

A complete batch data processing pipeline using Apache Spark to clean, transform, and analyze large CSV datasets. This project demonstrates ETL (Extract, Transform, Load) best practices, computes summary statistics, and persists results to PostgreSQL for querying and reporting.

**Author:** [Your Name]
**Date:** 2024
**Course:** [Your Course Name]

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Detailed Setup Instructions](#detailed-setup-instructions)
- [Pipeline Explanation](#pipeline-explanation)
- [Data Flow](#data-flow)
- [Aggregations Computed](#aggregations-computed)
- [Querying Results](#querying-results)
- [Configuration](#configuration)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Performance Optimization](#performance-optimization)
- [Future Enhancements](#future-enhancements)
- [References](#references)

---

## Project Overview

This project implements a **batch data processing pipeline** that processes sales transaction data through the following stages:

1. **Extract**: Load raw CSV data into Spark DataFrames
2. **Transform**: Clean, validate, and enrich the data
3. **Aggregate**: Compute business metrics and KPIs
4. **Load**: Persist results to PostgreSQL and Parquet files

### Use Case

A retail company needs to analyze their sales data to understand:
- Which product categories generate the most revenue
- Regional sales performance
- Customer purchasing patterns
- Daily sales trends
- Product popularity

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Spark Batch Analytics Pipeline                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │          │    │              │    │                      │  │
│  │  CSV     │───▶│  Apache      │───▶│  PostgreSQL          │  │
│  │  Files   │    │  Spark       │    │  Database            │  │
│  │          │    │  (PySpark)   │    │                      │  │
│  └──────────┘    └──────────────┘    └──────────────────────┘  │
│                         │                                       │
│                         │                                       │
│                         ▼                                       │
│                  ┌──────────────┐                               │
│                  │              │                               │
│                  │  Parquet     │                               │
│                  │  Files       │                               │
│                  │              │                               │
│                  └──────────────┘                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Docker Environment                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────┐         ┌─────────────────────────┐        │
│  │                 │         │                         │        │
│  │  spark-etl      │────────▶│  postgres               │        │
│  │  Container      │  JDBC   │  Container              │        │
│  │                 │         │                         │        │
│  └─────────────────┘         └─────────────────────────┘        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Features

- **Data Cleaning**: Remove duplicates, handle null values, standardize text
- **Data Transformation**: Calculate derived fields, categorize data
- **Aggregation Jobs**: Compute 7 different summary statistics
- **Dual Storage**: Save to both PostgreSQL and Parquet
- **Docker Support**: Fully containerized for reproducible execution
- **Memory Optimized**: Configured for machines with 8GB RAM
- **Comprehensive Logging**: Track pipeline progress and errors
- **Unit Tests**: Pytest-based test suite

---

## Prerequisites

### Hardware Requirements
- **Minimum**: 8GB RAM (optimized for Mac M2 Air)
- **CPU**: 2+ cores recommended
- **Storage**: 2GB free disk space

### Software Requirements

| Software | Version | Installation |
|----------|---------|--------------|
| Docker Desktop | 4.x+ | [Download](https://www.docker.com/products/docker-desktop/) |
| Git | 2.x+ | [Download](https://git-scm.com/downloads) |

> **Note**: You don't need to install Python, Spark, or PostgreSQL locally. Everything runs in Docker containers!

### For Mac M2 Users

Docker Desktop is fully compatible with Apple Silicon. Make sure to:
1. Download the Apple Silicon version of Docker Desktop
2. Allocate at least 4GB RAM to Docker (Preferences → Resources)

---

## Project Structure

```
spark-batch-analytics-pipeline/
│
├── README.md                    # This documentation file
├── Dockerfile                   # Spark container configuration
├── docker-compose.yml           # Multi-container orchestration
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore rules
├── .env.example                 # Environment variables template
│
├── src/                         # Source code
│   └── etl_pipeline.py          # Main ETL pipeline code
│
├── data/                        # Data directory
│   ├── raw/                     # Input CSV files
│   │   └── sales_data.csv       # Sample sales data
│   └── processed/               # Output files (generated)
│
├── config/                      # Configuration files
│   └── pipeline_config.yaml     # Pipeline settings
│
├── scripts/                     # Utility scripts
│   ├── init_db.sql              # PostgreSQL schema
│   └── run_pipeline.sh          # Pipeline runner script
│
├── tests/                       # Unit tests
│   └── test_etl_pipeline.py     # Pytest test cases
│
├── docker/                      # Additional Docker configs
│
└── docs/                        # Additional documentation
```

---

## Quick Start

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/spark-batch-analytics-pipeline.git
cd spark-batch-analytics-pipeline
```

### Step 2: Start the Pipeline

```bash
# Build and run everything with one command
docker-compose up --build
```

### Step 3: View Results

```bash
# Connect to PostgreSQL
docker exec -it analytics-postgres psql -U analytics_user -d analytics_db

# Run a sample query
SELECT * FROM agg_sales_by_category;
```

That's it! The pipeline will process the data and display results.

---

## Detailed Setup Instructions

### For Mac (M2 Air with 8GB RAM)

#### 1. Install Docker Desktop

```bash
# Download from: https://www.docker.com/products/docker-desktop/
# Or using Homebrew:
brew install --cask docker
```

#### 2. Configure Docker Resources

Open Docker Desktop → Settings → Resources:
- **Memory**: 4GB (minimum)
- **CPUs**: 2 (minimum)
- **Disk**: 20GB

#### 3. Clone and Navigate

```bash
git clone https://github.com/yourusername/spark-batch-analytics-pipeline.git
cd spark-batch-analytics-pipeline
```

#### 4. Set Up Environment Variables

```bash
# Copy the example file
cp .env.example .env

# Edit if needed (defaults work fine)
```

#### 5. Build Docker Images

```bash
docker-compose build
```

#### 6. Run the Pipeline

```bash
# Run the complete pipeline
docker-compose up
```

#### 7. Verify Results

```bash
# Check if containers are running
docker ps

# View logs
docker-compose logs spark-etl
```

### For Windows

The same steps apply. Use PowerShell or Git Bash:

```powershell
# Clone
git clone https://github.com/yourusername/spark-batch-analytics-pipeline.git
cd spark-batch-analytics-pipeline

# Build and run
docker-compose up --build
```

---

## Pipeline Explanation

### The ETL Process

#### 1. Extract Phase (`extract_data`)

```python
def extract_data(self, file_path):
    """Load CSV data with explicit schema for better performance."""
    df = self.spark.read \
        .option("header", "true") \
        .schema(self.define_schema()) \
        .csv(file_path)
    return df
```

**Why explicit schema?**
- Faster than schema inference
- Ensures data type consistency
- Catches data quality issues early

#### 2. Clean Phase (`clean_data`)

The cleaning process handles:

| Issue | Solution |
|-------|----------|
| Duplicate records | Remove by transaction_id |
| Whitespace in text | Trim all string columns |
| Null quantities | Default to 1 |
| Null prices | Default to 0 |
| Invalid records | Filter negative values |
| Date format | Convert string to DateType |

```python
# Example: Handle null values
df_cleaned = df.withColumn(
    "quantity",
    when(col("quantity").isNull(), lit(1)).otherwise(col("quantity"))
)
```

#### 3. Transform Phase (`transform_data`)

Creates derived columns for analysis:

| Column | Calculation |
|--------|-------------|
| `total_amount` | quantity × unit_price |
| `year`, `month` | Extracted from date |
| `day_of_week` | 1 (Sunday) to 7 (Saturday) |
| `price_tier` | Budget/Mid-Range/Premium/Luxury |
| `transaction_size` | Small/Medium/Large |

```python
# Price tier categorization
df_transformed = df.withColumn(
    "price_tier",
    when(col("unit_price") < 50, "Budget")
    .when(col("unit_price") < 200, "Mid-Range")
    .when(col("unit_price") < 500, "Premium")
    .otherwise("Luxury")
)
```

#### 4. Aggregate Phase (`compute_aggregations`)

Computes 7 aggregation tables for business insights.

#### 5. Load Phase

Saves results to:
- **PostgreSQL**: For SQL querying and reporting
- **Parquet**: For efficient storage and future processing

---

## Data Flow

```
sales_data.csv
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ EXTRACT: Load 50 raw records with schema validation         │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ CLEAN: Remove duplicates, handle nulls, validate data       │
│ • Deduplicate by transaction_id                             │
│ • Trim whitespace from text fields                          │
│ • Handle null quantities (default: 1)                       │
│ • Filter invalid records                                    │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ TRANSFORM: Add derived columns                              │
│ • total_amount = quantity × unit_price                      │
│ • Extract year, month, day_of_week                          │
│ • Categorize price_tier and transaction_size                │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ AGGREGATE: Compute summary statistics                       │
│ • Sales by category, region, payment method                 │
│ • Daily sales summary                                       │
│ • Customer and product performance                          │
└─────────────────────────────────────────────────────────────┘
      │
      ├──────────────────────┬──────────────────────┐
      ▼                      ▼                      ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  PostgreSQL  │     │   Parquet    │     │   Console    │
│   Tables     │     │    Files     │     │   Report     │
└──────────────┘     └──────────────┘     └──────────────┘
```

---

## Aggregations Computed

### 1. Sales by Category
```sql
SELECT category, total_transactions, total_revenue, avg_transaction_value
FROM agg_sales_by_category
ORDER BY total_revenue DESC;
```

| Metric | Description |
|--------|-------------|
| total_transactions | Count of transactions per category |
| total_units_sold | Sum of quantities |
| total_revenue | Sum of total_amount |
| avg_transaction_value | Average transaction value |
| avg_unit_price | Average price per unit |

### 2. Sales by Region
Geographic performance analysis across North, South, East, West regions.

### 3. Sales by Payment Method
Payment preference analysis: Credit Card, Debit Card, PayPal, Cash.

### 4. Daily Sales Summary
Time-series data for trend analysis.

### 5. Customer Summary
Customer lifetime value and purchase frequency.

### 6. Product Performance
Best-selling products and revenue generators.

### 7. Store Performance
Per-store metrics for location analysis.

---

## Querying Results

### Connect to PostgreSQL

```bash
docker exec -it analytics-postgres psql -U analytics_user -d analytics_db
```

### Sample Queries

```sql
-- Top 5 categories by revenue
SELECT category, total_revenue
FROM agg_sales_by_category
ORDER BY total_revenue DESC
LIMIT 5;

-- Regional comparison
SELECT region, total_transactions, total_revenue
FROM agg_sales_by_region;

-- Top customers
SELECT customer_id, total_purchases, total_spent
FROM agg_customer_summary
ORDER BY total_spent DESC
LIMIT 10;

-- Daily revenue trend
SELECT transaction_date, daily_revenue
FROM agg_daily_sales
ORDER BY transaction_date;

-- Most popular products
SELECT product_name, total_units_sold, total_revenue
FROM agg_product_performance
ORDER BY total_units_sold DESC
LIMIT 10;
```

### Export Query Results

```bash
# Export to CSV
docker exec -it analytics-postgres psql -U analytics_user -d analytics_db \
  -c "COPY (SELECT * FROM agg_sales_by_category) TO STDOUT WITH CSV HEADER" \
  > category_report.csv
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | postgres | Database hostname |
| `POSTGRES_PORT` | 5432 | Database port |
| `POSTGRES_DB` | analytics_db | Database name |
| `POSTGRES_USER` | analytics_user | Database user |
| `POSTGRES_PASSWORD` | analytics_pass | Database password |
| `SPARK_DRIVER_MEMORY` | 2g | Spark driver memory |
| `SPARK_EXECUTOR_MEMORY` | 1g | Spark executor memory |

### Modifying Spark Settings

Edit `docker-compose.yml` or set environment variables:

```yaml
environment:
  - SPARK_DRIVER_MEMORY=2g    # Increase for larger datasets
  - SPARK_EXECUTOR_MEMORY=1g
```

### Using Your Own Data

1. Place your CSV file in `data/raw/`
2. Update the schema in `src/etl_pipeline.py` → `define_schema()`
3. Modify column names in transformation functions
4. Rebuild and run:

```bash
docker-compose up --build
```

---

## Testing

### Run Unit Tests

```bash
# Build test environment
docker-compose build

# Run tests
docker-compose run --rm spark-etl pytest /app/tests/ -v
```

### Test Locally (Optional)

If you have Python and PySpark installed locally:

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v
```

### Test Coverage

```bash
pytest tests/ --cov=src --cov-report=html
```

---

## Troubleshooting

### Common Issues

#### 1. "Cannot connect to Docker daemon"

**Solution**: Start Docker Desktop

```bash
# Mac
open -a Docker

# Then wait 30 seconds and retry
```

#### 2. "Port 5432 already in use"

**Solution**: Stop existing PostgreSQL or change port

```bash
# Find process using port
lsof -i :5432

# Or change port in docker-compose.yml
ports:
  - "5433:5432"  # Use 5433 instead
```

#### 3. "Out of memory"

**Solution**: Reduce Spark memory settings

```yaml
# In docker-compose.yml
environment:
  - SPARK_DRIVER_MEMORY=1g
  - SPARK_EXECUTOR_MEMORY=512m
```

#### 4. "Connection refused to postgres"

**Solution**: Wait for PostgreSQL to start

```bash
# Check if postgres is healthy
docker-compose ps

# View postgres logs
docker-compose logs postgres
```

#### 5. Build fails with "no space left on device"

**Solution**: Clean Docker resources

```bash
# Remove unused images and containers
docker system prune -a

# Remove unused volumes
docker volume prune
```

### View Logs

```bash
# All services
docker-compose logs

# Specific service
docker-compose logs spark-etl

# Follow logs in real-time
docker-compose logs -f spark-etl
```

### Reset Everything

```bash
# Stop and remove all containers, networks, volumes
docker-compose down -v

# Rebuild from scratch
docker-compose up --build
```

---

## Performance Optimization

### For 8GB RAM Machines

The pipeline is already optimized, but here are additional tips:

1. **Reduce Shuffle Partitions**
   ```python
   .config("spark.sql.shuffle.partitions", "4")
   ```

2. **Use Coalesce for Small Outputs**
   ```python
   df.coalesce(1).write.parquet(output_path)
   ```

3. **Cache Intermediate DataFrames**
   ```python
   transformed_df.cache()
   # ... use transformed_df multiple times ...
   transformed_df.unpersist()
   ```

4. **Filter Early**
   ```python
   # Filter before joining or aggregating
   df.filter(col("region") == "North").groupBy(...)
   ```

### For Larger Datasets

If processing larger files (>100MB):

1. Increase Docker memory allocation
2. Consider Spark cluster mode
3. Partition data by date or category

---

## Future Enhancements

Potential improvements for this project:

- [ ] Add streaming support with Spark Structured Streaming
- [ ] Implement data quality monitoring with Great Expectations
- [ ] Add visualization dashboard with Apache Superset
- [ ] Deploy to cloud (AWS EMR, GCP Dataproc, Azure HDInsight)
- [ ] Add CI/CD pipeline with GitHub Actions
- [ ] Implement incremental processing
- [ ] Add email/Slack notifications

---

## References

### Documentation

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Documentation](https://docs.docker.com/)

### Tutorials

- [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)

### Tools Used

| Tool | Version | Purpose |
|------|---------|---------|
| Apache Spark | 3.5.0 | Distributed data processing |
| PySpark | 3.5.0 | Python API for Spark |
| PostgreSQL | 15 | Relational database |
| Docker | 24+ | Containerization |
| Python | 3.9+ | Programming language |

---

## License

This project is created for educational purposes.

---

## Contact

For questions or feedback:
- **Email**: [your.email@university.edu]
- **GitHub**: [github.com/yourusername]

---

**Happy Data Engineering!**
