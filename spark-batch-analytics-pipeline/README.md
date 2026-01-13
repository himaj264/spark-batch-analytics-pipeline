# Spark Batch Analytics Pipeline

A complete batch data processing pipeline using Apache Spark to clean, transform, and analyze large CSV datasets. This project demonstrates ETL (Extract, Transform, Load) best practices, computes summary statistics, and persists results to PostgreSQL for querying and reporting.



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



### Software Requirements

| Software | Version | Installation |
|----------|---------|--------------|
| Docker Desktop | 4.x+ | [Download](https://www.docker.com/products/docker-desktop/) |
| Git | 2.x+ | [Download](https://git-scm.com/downloads) |





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




### Tools Used

| Tool | Version | Purpose |
|------|---------|---------|
| Apache Spark | 3.5.0 | Distributed data processing |
| PySpark | 3.5.0 | Python API for Spark |
| PostgreSQL | 15 | Relational database |
| Docker | 24+ | Containerization |
| Python | 3.9+ | Programming language |

