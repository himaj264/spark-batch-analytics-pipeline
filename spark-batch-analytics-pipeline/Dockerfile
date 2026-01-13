# ============================================
# Spark Batch Analytics Pipeline - Dockerfile
# ============================================
# Optimized for machines with limited memory (8GB RAM)
# Base image: Apache Spark with Python support

FROM apache/spark-py:3.5.0

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}
ENV PATH=${SPARK_HOME}/bin:${PATH}
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Switch to root for installations
USER root

# Create necessary directories
RUN mkdir -p /app/data/raw /app/data/processed /app/jars /app/src /app/logs

# Download PostgreSQL JDBC driver
RUN curl -L -o /app/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Copy requirements and install Python dependencies
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY src/ /app/src/
COPY data/raw/ /app/data/raw/
COPY config/ /app/config/

# Set working directory
WORKDIR /app

# Set proper permissions
RUN chmod -R 755 /app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import pyspark; print('Spark available')" || exit 1

# Default command - run the ETL pipeline
CMD ["spark-submit", \
     "--master", "local[2]", \
     "--driver-memory", "2g", \
     "--executor-memory", "1g", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "--conf", "spark.default.parallelism=4", \
     "--jars", "/app/jars/postgresql-42.6.0.jar", \
     "/app/src/etl_pipeline.py"]
