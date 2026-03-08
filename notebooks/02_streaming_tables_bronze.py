# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 02 - Streaming Tables (Bronze Layer)
# MAGIC
# MAGIC ## Overview
# MAGIC Master the `@dp.table` decorator for continuous, incremental data ingestion.
# MAGIC This is your Bronze layer pattern - raw data with full history preservation.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - When to use streaming vs batch processing
# MAGIC - Auto Loader configuration and features
# MAGIC - Checkpoint management
# MAGIC - Schema evolution handling
# MAGIC - Performance optimization for streaming
# MAGIC
# MAGIC ## Author
# MAGIC Ahmed Mahmoud - DataMindAI
# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==============================================================================
# SECTION 1: Basic Streaming Table
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## The @dp.table Decorator
# MAGIC
# MAGIC A streaming table is:
# MAGIC - Incremental and append-only
# MAGIC - Processes only new data since last checkpoint
# MAGIC - Ideal for raw data ingestion (Bronze layer)
# MAGIC - Uses Auto Loader for intelligent file detection
# COMMAND ----------
@dp.table(
    comment="Raw customer data from S3",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    """
    Basic streaming table example.
    
    Key Features:
    - cloudFiles format enables Auto Loader
    - Automatically detects new files
    - Handles schema inference or evolution
    - Maintains checkpoints for exactly-once processing
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/customers/")

# ==============================================================================
# SECTION 2: Auto Loader with Schema Location
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Auto Loader Best Practices
# MAGIC
# MAGIC Always specify schemaLocation for production pipelines.
# MAGIC This prevents re-inferring schema on every run and reduces costs.
# COMMAND ----------
@dp.table(
    comment="Raw order data with explicit schema location",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "order_date"
    }
)
def bronze_orders():
    """
    Production-ready streaming table.
    
    Benefits of schemaLocation:
    - Schema inferred only once
    - Stored for reuse across runs
    - Reduces processing time
    - Catches schema changes proactively
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/mnt/schemas/orders") \
        .load("/mnt/raw/orders/")

# ==============================================================================
# SECTION 3: Auto Loader with Explicit Schema
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Explicit Schema Definition
# MAGIC
# MAGIC For maximum performance and control, define your schema explicitly.
# MAGIC This is the BEST PRACTICE for production pipelines.
# COMMAND ----------
# Define schema once
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

@dp.table(
    comment="Customer data with explicit schema",
    table_properties={"quality": "bronze"}
)
def bronze_customers_explicit():
    """
    Best practice: Explicit schema definition.
    
    Advantages:
    - Zero schema inference cost
    - Validates incoming data format
    - Immediate error detection
    - Predictable behavior
    """
    return spark.readStream.format("cloudFiles") \
        .schema(customers_schema) \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/customers/")

# ==============================================================================
# SECTION 4: Multiple File Formats
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Working with Different File Formats
# MAGIC
# MAGIC Auto Loader supports: JSON, CSV, Parquet, Avro, ORC, and more.
# COMMAND ----------
# CSV with options
@dp.table(
    comment="Product data from CSV files"
)
def bronze_products_csv():
    """
    CSV ingestion with Auto Loader.
    
    Common CSV options:
    - header: First row contains column names
    - inferSchema: Auto-detect data types
    - delimiter: Field separator
    - multiLine: Handle multi-line values
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/mnt/raw/products/")

# Parquet (most efficient)
@dp.table(
    comment="Transaction data from Parquet"
)
def bronze_transactions_parquet():
    """
    Parquet ingestion - most efficient format.
    
    Advantages:
    - Columnar storage
    - Built-in compression
    - Schema is embedded
    - Predicate pushdown support
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .load("/mnt/raw/transactions/")

# ==============================================================================
# SECTION 5: Schema Evolution
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Handling Schema Changes
# MAGIC
# MAGIC Auto Loader can handle schema evolution gracefully.
# COMMAND ----------
@dp.table(
    comment="Event data with schema evolution",
    table_properties={"quality": "bronze"}
)
def bronze_events_evolving():
    """
    Enable schema evolution for evolving data sources.
    
    Options:
    - schemaEvolutionMode: 'addNewColumns' or 'rescue'
    - rescue: Store unparseable data in _rescued_data column
    - failOnDataLoss: false to handle missing files gracefully
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/mnt/schemas/events") \
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
        .option("rescuedDataColumn", "_rescued_data") \
        .load("/mnt/raw/events/")

# ==============================================================================
# SECTION 6: Advanced: Partition Discovery
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Working with Partitioned Data
# MAGIC
# MAGIC If your source data is partitioned, Auto Loader can discover partitions.
# COMMAND ----------
@dp.table(
    comment="Sales data partitioned by year and month"
)
def bronze_sales_partitioned():
    """
    Partition columns are automatically added.
    
    Example directory structure:
    /mnt/raw/sales/year=2024/month=01/
    /mnt/raw/sales/year=2024/month=02/
    
    Partition columns (year, month) are added automatically.
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("pathGlobFilter", "*.parquet") \
        .load("/mnt/raw/sales/year=*/month=*/")

# ==============================================================================
# SECTION 7: Adding Metadata Columns
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Enriching with Ingestion Metadata
# MAGIC
# MAGIC Track when and from where data was ingested.
# COMMAND ----------
@dp.table(
    comment="Logs with ingestion metadata"
)
def bronze_logs_with_metadata():
    """
    Add metadata columns for auditing and debugging.
    
    Metadata columns:
    - _metadata.file_path: Source file location
    - _metadata.file_name: Source file name
    - _metadata.file_modification_time: File last modified timestamp
    - current_timestamp(): Processing timestamp
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/logs/") \
        .select(
            "*",
            col("_metadata.file_path").alias("source_file"),
            col("_metadata.file_modification_time").alias("file_timestamp"),
            current_timestamp().alias("ingestion_timestamp")
        )

# ==============================================================================
# SECTION 8: Performance Optimization
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Optimizing Streaming Performance
# MAGIC
# MAGIC Tips for better performance:
# COMMAND ----------
@dp.table(
    comment="High-performance streaming table",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_high_volume():
    """
    Performance optimizations:
    
    1. maxFilesPerTrigger: Limit files processed per batch
    2. maxBytesPerTrigger: Limit data volume per batch
    3. delta.autoOptimize: Enable automatic optimization
    4. Schema caching: Always use explicit schema
    5. Partitioning: Use liquid clustering for new tables
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.maxFilesPerTrigger", "1000") \
        .option("cloudFiles.maxBytesPerTrigger", "1g") \
        .load("/mnt/raw/high_volume/")

# ==============================================================================
# SECTION 9: Error Handling
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Handling Bad Records
# MAGIC
# MAGIC Strategies for dealing with malformed data.
# COMMAND ----------
@dp.table(
    comment="Robust ingestion with error handling"
)
def bronze_with_error_handling():
    """
    Graceful error handling options:
    
    - mode: PERMISSIVE (default) - Sets corrupt records to null
    - mode: DROPMALFORMED - Drops corrupt records
    - mode: FAILFAST - Aborts on corrupt records
    - columnNameOfCorruptRecord: Store bad data in this column
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .load("/mnt/raw/potentially_bad_data/")

# ==============================================================================
# KEY CONCEPTS SUMMARY
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming Tables Best Practices
# MAGIC
# MAGIC ### ✅ DO:
# MAGIC 1. Always use explicit schema or schemaLocation in production
# MAGIC 2. Set appropriate maxFilesPerTrigger for cost control
# MAGIC 3. Add ingestion metadata columns for debugging
# MAGIC 4. Use rescue columns for schema evolution
# MAGIC 5. Enable Auto Optimize for Delta tables
# MAGIC 6. Use Parquet format when possible (most efficient)
# MAGIC
# MAGIC ### ❌ DON'T:
# MAGIC 1. Use schema inference without caching (costs money)
# MAGIC 2. Process unlimited files per trigger (costs blow up)
# MAGIC 3. Use .collect(), .count(), or .save() in definitions
# MAGIC 4. Skip error handling for production pipelines
# MAGIC 5. Ignore checkpoint management
# MAGIC
# MAGIC ### 💡 Pro Tips:
# MAGIC - Monitor your checkpoints directory size
# MAGIC - Use liquid clustering instead of traditional partitioning
# MAGIC - Set retention policies on bronze tables (VACUUM)
# MAGIC - Test with small datasets first
# MAGIC - Use serverless compute for cost efficiency
# MAGIC
# MAGIC ## Performance Checklist
# MAGIC
# MAGIC - [ ] Explicit schema defined
# MAGIC - [ ] Schema location configured
# MAGIC - [ ] maxFilesPerTrigger set appropriately
# MAGIC - [ ] Auto Optimize enabled
# MAGIC - [ ] Error handling configured
# MAGIC - [ ] Metadata columns added for debugging
# MAGIC - [ ] Checkpoint location monitored
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Move to `03_materialized_views_silver.py` to learn about transforming
# MAGIC this raw bronze data into clean, validated silver tables.
