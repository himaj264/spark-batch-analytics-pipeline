"""
Spark Batch Analytics Pipeline - ETL Job
=========================================
This module implements a complete ETL (Extract, Transform, Load) pipeline
using Apache Spark for processing sales transaction data.

Author: [Your Name]
Date: 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, round as spark_round, when, lit,
    to_date, year, month, dayofweek, date_format,
    trim, upper, lower, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkETLPipeline:
    """
    A comprehensive ETL pipeline class that handles:
    - Data extraction from CSV files
    - Data cleaning and transformation
    - Aggregation computations
    - Loading results to PostgreSQL
    """

    def __init__(self, app_name="SalesBatchAnalytics"):
        """
        Initialize the Spark session with optimized configurations
        for machines with limited memory (8GB RAM).

        Args:
            app_name (str): Name of the Spark application
        """
        logger.info(f"Initializing Spark session: {app_name}")

        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4") \
            .config("spark.driver.extraClassPath", "/app/jars/postgresql-42.6.0.jar") \
            .config("spark.jars", "/app/jars/postgresql-42.6.0.jar") \
            .getOrCreate()

        # Set log level to reduce verbosity
        self.spark.sparkContext.setLogLevel("WARN")

        logger.info("Spark session initialized successfully")

    def define_schema(self):
        """
        Define explicit schema for the sales data.
        Using explicit schema improves performance and ensures data quality.

        Returns:
            StructType: Schema definition for sales data
        """
        return StructType([
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

    def extract_data(self, file_path):
        """
        Extract data from CSV file with proper error handling.

        Args:
            file_path (str): Path to the input CSV file

        Returns:
            DataFrame: Raw data loaded from CSV
        """
        logger.info(f"Extracting data from: {file_path}")

        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(self.define_schema()) \
                .csv(file_path)

            record_count = df.count()
            logger.info(f"Extracted {record_count} records")

            return df

        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise

    def clean_data(self, df):
        """
        Clean and standardize the raw data.

        Cleaning operations:
        - Remove duplicates based on transaction_id
        - Handle null values
        - Standardize text fields (trim whitespace, consistent casing)
        - Convert date strings to proper date type
        - Validate numeric ranges

        Args:
            df (DataFrame): Raw input DataFrame

        Returns:
            DataFrame: Cleaned DataFrame
        """
        logger.info("Starting data cleaning process")

        # Remove duplicates
        df_cleaned = df.dropDuplicates(["transaction_id"])

        # Clean text fields - trim whitespace
        df_cleaned = df_cleaned \
            .withColumn("product_name", trim(col("product_name"))) \
            .withColumn("category", trim(col("category"))) \
            .withColumn("region", trim(col("region"))) \
            .withColumn("payment_method", trim(col("payment_method")))

        # Standardize category names (capitalize first letter)
        df_cleaned = df_cleaned.withColumn(
            "category",
            when(col("category").isNotNull(),
                 regexp_replace(col("category"), "^(.)", upper(regexp_replace(col("category"), "^(.)", "$1")))
            ).otherwise(lit("Unknown"))
        )

        # Convert transaction_date to DateType
        df_cleaned = df_cleaned.withColumn(
            "transaction_date",
            to_date(col("transaction_date"), "yyyy-MM-dd")
        )

        # Handle null quantities - default to 1
        df_cleaned = df_cleaned.withColumn(
            "quantity",
            when(col("quantity").isNull(), lit(1)).otherwise(col("quantity"))
        )

        # Handle null prices - default to 0
        df_cleaned = df_cleaned.withColumn(
            "unit_price",
            when(col("unit_price").isNull(), lit(0.0)).otherwise(col("unit_price"))
        )

        # Filter out invalid records (negative quantities or prices)
        df_cleaned = df_cleaned.filter(
            (col("quantity") > 0) & (col("unit_price") >= 0)
        )

        logger.info(f"Data cleaning complete. Records after cleaning: {df_cleaned.count()}")

        return df_cleaned

    def transform_data(self, df):
        """
        Apply business transformations to create derived columns.

        Transformations:
        - Calculate total amount (quantity * unit_price)
        - Extract date components (year, month, day of week)
        - Add day name for readability
        - Create price tier categories

        Args:
            df (DataFrame): Cleaned DataFrame

        Returns:
            DataFrame: Transformed DataFrame with additional columns
        """
        logger.info("Starting data transformation")

        # Calculate total transaction amount
        df_transformed = df.withColumn(
            "total_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )

        # Extract date components
        df_transformed = df_transformed \
            .withColumn("year", year(col("transaction_date"))) \
            .withColumn("month", month(col("transaction_date"))) \
            .withColumn("day_of_week", dayofweek(col("transaction_date"))) \
            .withColumn("day_name", date_format(col("transaction_date"), "EEEE"))

        # Create price tier categories
        df_transformed = df_transformed.withColumn(
            "price_tier",
            when(col("unit_price") < 50, "Budget")
            .when((col("unit_price") >= 50) & (col("unit_price") < 200), "Mid-Range")
            .when((col("unit_price") >= 200) & (col("unit_price") < 500), "Premium")
            .otherwise("Luxury")
        )

        # Create transaction size category
        df_transformed = df_transformed.withColumn(
            "transaction_size",
            when(col("total_amount") < 100, "Small")
            .when((col("total_amount") >= 100) & (col("total_amount") < 500), "Medium")
            .otherwise("Large")
        )

        logger.info("Data transformation complete")

        return df_transformed

    def compute_aggregations(self, df):
        """
        Compute various aggregations for analytics and reporting.

        Aggregations computed:
        1. Sales by category
        2. Sales by region
        3. Sales by payment method
        4. Daily sales summary
        5. Customer purchase summary
        6. Product performance

        Args:
            df (DataFrame): Transformed DataFrame

        Returns:
            dict: Dictionary containing all aggregation DataFrames
        """
        logger.info("Computing aggregations")

        aggregations = {}

        # 1. Sales by Category
        aggregations['sales_by_category'] = df.groupBy("category") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_units_sold"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
                spark_round(avg("total_amount"), 2).alias("avg_transaction_value"),
                spark_round(avg("unit_price"), 2).alias("avg_unit_price")
            ) \
            .orderBy(col("total_revenue").desc())

        # 2. Sales by Region
        aggregations['sales_by_region'] = df.groupBy("region") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_units_sold"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
                spark_round(avg("total_amount"), 2).alias("avg_transaction_value")
            ) \
            .orderBy(col("total_revenue").desc())

        # 3. Sales by Payment Method
        aggregations['sales_by_payment'] = df.groupBy("payment_method") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
                spark_round(avg("total_amount"), 2).alias("avg_transaction_value")
            ) \
            .orderBy(col("total_transactions").desc())

        # 4. Daily Sales Summary
        aggregations['daily_sales'] = df.groupBy("transaction_date", "day_name") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("quantity").alias("total_units_sold"),
                spark_round(spark_sum("total_amount"), 2).alias("daily_revenue"),
                spark_round(avg("total_amount"), 2).alias("avg_transaction_value")
            ) \
            .orderBy("transaction_date")

        # 5. Customer Purchase Summary
        aggregations['customer_summary'] = df.groupBy("customer_id") \
            .agg(
                count("transaction_id").alias("total_purchases"),
                spark_sum("quantity").alias("total_items_bought"),
                spark_round(spark_sum("total_amount"), 2).alias("total_spent"),
                spark_round(avg("total_amount"), 2).alias("avg_purchase_value"),
                spark_min("transaction_date").alias("first_purchase"),
                spark_max("transaction_date").alias("last_purchase")
            ) \
            .orderBy(col("total_spent").desc())

        # 6. Product Performance
        aggregations['product_performance'] = df.groupBy("product_id", "product_name", "category") \
            .agg(
                count("transaction_id").alias("times_purchased"),
                spark_sum("quantity").alias("total_units_sold"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
                spark_round(avg("unit_price"), 2).alias("avg_selling_price")
            ) \
            .orderBy(col("total_revenue").desc())

        # 7. Store Performance
        aggregations['store_performance'] = df.groupBy("store_id", "region") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
                spark_round(avg("total_amount"), 2).alias("avg_transaction_value")
            ) \
            .orderBy(col("total_revenue").desc())

        logger.info("Aggregations computed successfully")

        return aggregations

    def save_to_postgres(self, df, table_name, db_config):
        """
        Save DataFrame to PostgreSQL database.

        Args:
            df (DataFrame): DataFrame to save
            table_name (str): Target table name
            db_config (dict): Database connection configuration
        """
        logger.info(f"Saving data to PostgreSQL table: {table_name}")

        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", db_config['user']) \
                .option("password", db_config['password']) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

            logger.info(f"Successfully saved {df.count()} records to {table_name}")

        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {str(e)}")
            raise

    def save_to_parquet(self, df, output_path, partition_cols=None):
        """
        Save DataFrame to Parquet format for efficient storage.

        Args:
            df (DataFrame): DataFrame to save
            output_path (str): Output directory path
            partition_cols (list): Optional columns to partition by
        """
        logger.info(f"Saving data to Parquet: {output_path}")

        try:
            writer = df.write.mode("overwrite")

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer.parquet(output_path)

            logger.info(f"Successfully saved data to {output_path}")

        except Exception as e:
            logger.error(f"Error saving to Parquet: {str(e)}")
            raise

    def generate_summary_report(self, df, aggregations):
        """
        Generate a summary report of the processed data.

        Args:
            df (DataFrame): Transformed DataFrame
            aggregations (dict): Computed aggregations
        """
        print("\n" + "="*60)
        print("SALES DATA ANALYTICS SUMMARY REPORT")
        print("="*60)
        print(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-"*60)

        # Overall Statistics
        total_records = df.count()
        total_revenue = df.agg(spark_sum("total_amount")).collect()[0][0]
        avg_transaction = df.agg(avg("total_amount")).collect()[0][0]

        print(f"\nOVERALL STATISTICS:")
        print(f"  Total Transactions: {total_records:,}")
        print(f"  Total Revenue: ${total_revenue:,.2f}")
        print(f"  Average Transaction Value: ${avg_transaction:,.2f}")

        # Top Categories
        print(f"\nTOP CATEGORIES BY REVENUE:")
        aggregations['sales_by_category'].show(5, truncate=False)

        # Regional Performance
        print(f"\nREGIONAL PERFORMANCE:")
        aggregations['sales_by_region'].show(truncate=False)

        # Top Products
        print(f"\nTOP 5 PRODUCTS BY REVENUE:")
        aggregations['product_performance'].select(
            "product_name", "category", "total_units_sold", "total_revenue"
        ).show(5, truncate=False)

        print("\n" + "="*60)
        print("END OF REPORT")
        print("="*60 + "\n")

    def run_pipeline(self, input_path, output_base_path, db_config=None):
        """
        Execute the complete ETL pipeline.

        Args:
            input_path (str): Path to input CSV file
            output_base_path (str): Base path for output files
            db_config (dict): Optional database configuration

        Returns:
            dict: Pipeline execution results
        """
        logger.info("="*50)
        logger.info("STARTING ETL PIPELINE")
        logger.info("="*50)

        start_time = datetime.now()

        try:
            # Step 1: Extract
            raw_df = self.extract_data(input_path)

            # Step 2: Clean
            cleaned_df = self.clean_data(raw_df)

            # Step 3: Transform
            transformed_df = self.transform_data(cleaned_df)

            # Cache transformed data for multiple operations
            transformed_df.cache()

            # Step 4: Compute Aggregations
            aggregations = self.compute_aggregations(transformed_df)

            # Step 5: Save Results
            # Save transformed data to Parquet
            self.save_to_parquet(
                transformed_df,
                f"{output_base_path}/transformed_sales",
                partition_cols=["year", "month"]
            )

            # Save aggregations to Parquet
            for name, agg_df in aggregations.items():
                self.save_to_parquet(agg_df, f"{output_base_path}/aggregations/{name}")

            # Save to PostgreSQL if config provided
            if db_config:
                self.save_to_postgres(transformed_df, "sales_transactions", db_config)

                for name, agg_df in aggregations.items():
                    self.save_to_postgres(agg_df, f"agg_{name}", db_config)

            # Step 6: Generate Report
            self.generate_summary_report(transformed_df, aggregations)

            # Unpersist cached data
            transformed_df.unpersist()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info("="*50)
            logger.info(f"ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Total Duration: {duration:.2f} seconds")
            logger.info("="*50)

            return {
                "status": "success",
                "records_processed": transformed_df.count(),
                "duration_seconds": duration,
                "aggregations_computed": list(aggregations.keys())
            }

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

    def stop(self):
        """Stop the Spark session."""
        logger.info("Stopping Spark session")
        self.spark.stop()


def main():
    """
    Main entry point for the ETL pipeline.
    """
    # Configuration
    INPUT_PATH = "/app/data/raw/sales_data.csv"
    OUTPUT_PATH = "/app/data/processed"

    # PostgreSQL configuration
    DB_CONFIG = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "analytics_db"),
        "user": os.getenv("POSTGRES_USER", "analytics_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "analytics_pass")
    }

    # Initialize and run pipeline
    pipeline = SparkETLPipeline()

    try:
        results = pipeline.run_pipeline(INPUT_PATH, OUTPUT_PATH, DB_CONFIG)
        print(f"\nPipeline Results: {results}")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
