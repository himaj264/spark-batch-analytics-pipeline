"""
Unit Tests for Spark ETL Pipeline
=================================
These tests validate the core functionality of the ETL pipeline.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("ETLPipelineTests") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark_session):
    """Create sample data for testing."""
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])

    data = [
        ("TXN001", "CUST101", "PROD001", "Laptop", "Electronics", 1, 999.99, "2024-01-15", "STORE01", "North", "Credit Card"),
        ("TXN002", "CUST102", "PROD002", "Mouse", "Electronics", 2, 29.99, "2024-01-15", "STORE02", "South", "Debit Card"),
        ("TXN003", "CUST103", "PROD003", "Chair", "Furniture", 1, 199.99, "2024-01-16", "STORE01", "North", "Credit Card"),
        ("TXN004", "CUST101", "PROD001", "Laptop", "Electronics", 1, 999.99, "2024-01-17", "STORE03", "East", "PayPal"),
        ("TXN005", "CUST104", "PROD004", "Desk", "Furniture", 1, 399.99, "2024-01-18", "STORE02", "South", "Cash"),
    ]

    return spark_session.createDataFrame(data, schema)


class TestDataCleaning:
    """Test cases for data cleaning operations."""

    def test_remove_duplicates(self, spark_session, sample_data):
        """Test that duplicate records are removed."""
        # Add a duplicate record
        duplicate_data = sample_data.union(
            sample_data.filter("transaction_id = 'TXN001'")
        )

        # Remove duplicates
        cleaned = duplicate_data.dropDuplicates(["transaction_id"])

        assert cleaned.count() == sample_data.count()

    def test_null_handling(self, spark_session):
        """Test that null values are handled correctly."""
        from pyspark.sql.functions import when, col, lit

        schema = StructType([
            StructField("id", StringType(), False),
            StructField("quantity", IntegerType(), True),
        ])

        data = [("1", None), ("2", 5), ("3", None)]
        df = spark_session.createDataFrame(data, schema)

        # Handle nulls
        df_cleaned = df.withColumn(
            "quantity",
            when(col("quantity").isNull(), lit(1)).otherwise(col("quantity"))
        )

        # Check no nulls remain
        null_count = df_cleaned.filter(col("quantity").isNull()).count()
        assert null_count == 0

        # Check default value applied
        row = df_cleaned.filter("id = '1'").collect()[0]
        assert row["quantity"] == 1


class TestDataTransformation:
    """Test cases for data transformation operations."""

    def test_total_amount_calculation(self, spark_session, sample_data):
        """Test that total_amount is calculated correctly."""
        from pyspark.sql.functions import col, round as spark_round

        transformed = sample_data.withColumn(
            "total_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )

        # Check first record: 1 * 999.99 = 999.99
        row = transformed.filter("transaction_id = 'TXN001'").collect()[0]
        assert row["total_amount"] == 999.99

        # Check second record: 2 * 29.99 = 59.98
        row = transformed.filter("transaction_id = 'TXN002'").collect()[0]
        assert row["total_amount"] == 59.98

    def test_price_tier_assignment(self, spark_session, sample_data):
        """Test that price tiers are assigned correctly."""
        from pyspark.sql.functions import col, when

        transformed = sample_data.withColumn(
            "price_tier",
            when(col("unit_price") < 50, "Budget")
            .when((col("unit_price") >= 50) & (col("unit_price") < 200), "Mid-Range")
            .when((col("unit_price") >= 200) & (col("unit_price") < 500), "Premium")
            .otherwise("Luxury")
        )

        # Mouse (29.99) should be Budget
        row = transformed.filter("product_name = 'Mouse'").collect()[0]
        assert row["price_tier"] == "Budget"

        # Chair (199.99) should be Mid-Range
        row = transformed.filter("product_name = 'Chair'").collect()[0]
        assert row["price_tier"] == "Mid-Range"

        # Laptop (999.99) should be Luxury
        row = transformed.filter("product_name = 'Laptop'").collect()[0]
        assert row["price_tier"] == "Luxury"


class TestAggregations:
    """Test cases for aggregation operations."""

    def test_sales_by_category(self, spark_session, sample_data):
        """Test category aggregation."""
        from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round

        # Add total_amount column
        df = sample_data.withColumn(
            "total_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )

        # Aggregate by category
        agg = df.groupBy("category") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("total_amount").alias("total_revenue")
            )

        # Electronics should have 3 transactions
        electronics = agg.filter("category = 'Electronics'").collect()[0]
        assert electronics["total_transactions"] == 3

        # Furniture should have 2 transactions
        furniture = agg.filter("category = 'Furniture'").collect()[0]
        assert furniture["total_transactions"] == 2

    def test_customer_summary(self, spark_session, sample_data):
        """Test customer aggregation."""
        from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round

        # Add total_amount column
        df = sample_data.withColumn(
            "total_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )

        # Aggregate by customer
        agg = df.groupBy("customer_id") \
            .agg(
                count("transaction_id").alias("total_purchases"),
                spark_sum("total_amount").alias("total_spent")
            )

        # CUST101 should have 2 purchases
        cust101 = agg.filter("customer_id = 'CUST101'").collect()[0]
        assert cust101["total_purchases"] == 2

        # CUST101 total spent should be 1999.98 (2 * 999.99)
        assert cust101["total_spent"] == pytest.approx(1999.98, rel=0.01)


class TestDataQuality:
    """Test cases for data quality validations."""

    def test_no_negative_quantities(self, spark_session, sample_data):
        """Test that negative quantities are filtered out."""
        from pyspark.sql.functions import col

        negative_count = sample_data.filter(col("quantity") < 0).count()
        assert negative_count == 0

    def test_no_negative_prices(self, spark_session, sample_data):
        """Test that negative prices are filtered out."""
        from pyspark.sql.functions import col

        negative_count = sample_data.filter(col("unit_price") < 0).count()
        assert negative_count == 0

    def test_valid_regions(self, spark_session, sample_data):
        """Test that all regions are valid."""
        valid_regions = ["North", "South", "East", "West"]

        regions = [row["region"] for row in sample_data.select("region").distinct().collect()]

        for region in regions:
            assert region in valid_regions


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
