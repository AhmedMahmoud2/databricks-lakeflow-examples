"""
# 02 - Streaming Tables (Bronze Layer)

## Overview
Master the `@dp.table` decorator for continuous, incremental data ingestion.
This is your Bronze layer pattern - raw data with full history preservation.

## What You'll Learn
- When to use streaming vs batch processing
- Auto Loader configuration and features
- Checkpoint management
- Schema evolution handling
- Performance optimization for streaming

## Author
Ahmed Mahmoud - DataMindAI
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==============================================================================
# SECTION 1: Basic Streaming Table
# ==============================================================================

"""
## The @dp.table Decorator

A streaming table is:
- Incremental and append-only
- Processes only new data since last checkpoint
- Ideal for raw data ingestion (Bronze layer)
- Uses Auto Loader for intelligent file detection
"""

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

"""
## Auto Loader Best Practices

Always specify schemaLocation for production pipelines.
This prevents re-inferring schema on every run and reduces costs.
"""

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

"""
## Explicit Schema Definition

For maximum performance and control, define your schema explicitly.
This is the BEST PRACTICE for production pipelines.
"""

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

"""
## Working with Different File Formats

Auto Loader supports: JSON, CSV, Parquet, Avro, ORC, and more.
"""

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

"""
## Handling Schema Changes

Auto Loader can handle schema evolution gracefully.
"""

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

"""
## Working with Partitioned Data

If your source data is partitioned, Auto Loader can discover partitions.
"""

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

"""
## Enriching with Ingestion Metadata

Track when and from where data was ingested.
"""

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

"""
## Optimizing Streaming Performance

Tips for better performance:
"""

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

"""
## Handling Bad Records

Strategies for dealing with malformed data.
"""

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

"""
## Streaming Tables Best Practices

### ✅ DO:
1. Always use explicit schema or schemaLocation in production
2. Set appropriate maxFilesPerTrigger for cost control
3. Add ingestion metadata columns for debugging
4. Use rescue columns for schema evolution
5. Enable Auto Optimize for Delta tables
6. Use Parquet format when possible (most efficient)

### ❌ DON'T:
1. Use schema inference without caching (costs money)
2. Process unlimited files per trigger (costs blow up)
3. Use .collect(), .count(), or .save() in definitions
4. Skip error handling for production pipelines
5. Ignore checkpoint management

### 💡 Pro Tips:
- Monitor your checkpoints directory size
- Use liquid clustering instead of traditional partitioning
- Set retention policies on bronze tables (VACUUM)
- Test with small datasets first
- Use serverless compute for cost efficiency

## Performance Checklist

- [ ] Explicit schema defined
- [ ] Schema location configured
- [ ] maxFilesPerTrigger set appropriately
- [ ] Auto Optimize enabled
- [ ] Error handling configured
- [ ] Metadata columns added for debugging
- [ ] Checkpoint location monitored

## Next Steps

Move to `03_materialized_views_silver.py` to learn about transforming
this raw bronze data into clean, validated silver tables.
"""
