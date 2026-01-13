# Architecture Documentation

## System Overview

This document explains the technical architecture of the Spark Batch Analytics Pipeline.

## Components

### 1. Apache Spark

**What is Apache Spark?**

Apache Spark is a unified analytics engine for large-scale data processing. It provides:
- **Speed**: In-memory computing, up to 100x faster than Hadoop MapReduce
- **Ease of Use**: High-level APIs in Python, Java, Scala, R
- **Generality**: Supports SQL, streaming, machine learning, and graph processing

**Why Spark for this project?**

- Handles large datasets efficiently
- Provides powerful transformation APIs
- Supports reading from various data sources
- Industry-standard for batch processing

### 2. PySpark

PySpark is the Python API for Apache Spark. Key components used:

```python
from pyspark.sql import SparkSession       # Entry point
from pyspark.sql.functions import *        # Transformations
from pyspark.sql.types import *            # Schema definitions
```

**SparkSession**: The entry point for Spark functionality. Configured with:
- `spark.driver.memory`: Memory for the driver program
- `spark.executor.memory`: Memory for executors
- `spark.sql.shuffle.partitions`: Number of partitions for shuffles

### 3. PostgreSQL

**Role**: Persistent storage for processed data and aggregations.

**Why PostgreSQL?**
- ACID compliance for data integrity
- Rich SQL support for complex queries
- Excellent performance for analytical queries
- Free and open-source

### 4. Docker

**Purpose**: Containerization for reproducible deployment.

**Benefits**:
- Consistent environment across machines
- No manual installation of Spark, PostgreSQL
- Easy cleanup and reset
- Platform-independent (Mac, Windows, Linux)

## Data Model

### Input Schema

```
transaction_id   | VARCHAR  | Primary key
customer_id      | VARCHAR  | Customer identifier
product_id       | VARCHAR  | Product identifier
product_name     | VARCHAR  | Product name
category         | VARCHAR  | Product category
quantity         | INTEGER  | Units purchased
unit_price       | DOUBLE   | Price per unit
transaction_date | DATE     | Transaction date
store_id         | VARCHAR  | Store identifier
region           | VARCHAR  | Geographic region
payment_method   | VARCHAR  | Payment type
```

### Derived Columns

```
total_amount      = quantity × unit_price
year              = YEAR(transaction_date)
month             = MONTH(transaction_date)
day_of_week       = DAYOFWEEK(transaction_date)
day_name          = Day name (Monday, Tuesday, etc.)
price_tier        = Budget/Mid-Range/Premium/Luxury
transaction_size  = Small/Medium/Large
```

## Processing Pipeline

### Stage 1: Extraction

```python
spark.read.csv(file_path, schema=schema, header=True)
```

- Reads CSV with explicit schema (faster than inference)
- Validates data types during load
- Records count for monitoring

### Stage 2: Cleaning

Operations performed:
1. `dropDuplicates(["transaction_id"])` - Remove duplicate transactions
2. `trim()` - Remove whitespace from text fields
3. `when().otherwise()` - Handle null values with defaults
4. `filter()` - Remove invalid records (negative values)
5. `to_date()` - Convert date strings to proper DateType

### Stage 3: Transformation

```python
df.withColumn("total_amount", col("quantity") * col("unit_price"))
  .withColumn("year", year(col("transaction_date")))
  .withColumn("price_tier", when(condition, value))
```

### Stage 4: Aggregation

Seven aggregation jobs using `groupBy().agg()`:

| Aggregation | Group By | Metrics |
|-------------|----------|---------|
| sales_by_category | category | transactions, units, revenue, avg |
| sales_by_region | region | transactions, units, revenue, avg |
| sales_by_payment | payment_method | transactions, revenue, avg |
| daily_sales | transaction_date | transactions, units, revenue, avg |
| customer_summary | customer_id | purchases, items, spent, avg, first/last |
| product_performance | product_id | purchased, units, revenue, avg_price |
| store_performance | store_id | transactions, revenue, avg |

### Stage 5: Loading

**To PostgreSQL:**
```python
df.write.format("jdbc")
  .option("url", jdbc_url)
  .option("dbtable", table_name)
  .mode("overwrite")
  .save()
```

**To Parquet:**
```python
df.write.partitionBy("year", "month").parquet(output_path)
```

## Memory Configuration

For 8GB RAM machines:

| Component | Memory | Rationale |
|-----------|--------|-----------|
| PostgreSQL | 512MB | Minimal for small datasets |
| Spark Driver | 2GB | Main processing |
| Spark Executor | 1GB | Task execution |
| System | 4.5GB | OS and Docker overhead |

## Scalability Considerations

### Current Limitations
- Single-node Spark (local mode)
- Limited to ~1GB datasets with 8GB RAM

### Scaling Options

**Vertical Scaling:**
- Increase Docker memory allocation
- Use machine with more RAM

**Horizontal Scaling:**
- Deploy Spark on cluster (Standalone, YARN, Kubernetes)
- Use cloud services (AWS EMR, GCP Dataproc)

## Security Notes

For production use:
- Store credentials in secrets manager
- Use SSL for database connections
- Encrypt data at rest
- Implement access controls
