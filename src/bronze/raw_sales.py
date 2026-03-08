"""
Bronze Layer: Raw Sales Data Ingestion

This module demonstrates streaming ingestion using Databricks Pipelines.
It uses Auto Loader to continuously ingest JSON files from cloud storage.

Key Features:
- Streaming table with incremental processing
- Auto Loader for intelligent file detection
- Checkpoint-based recovery
- Append-only pattern for data lake
"""

from pyspark import pipelines as dp

@dp.table(
    comment="Raw sales data ingested from S3 using Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def raw_sales():
    """
    Ingest raw sales data from JSON files in S3.
    
    This streaming table:
    - Processes only new files since last checkpoint
    - Maintains append-only history
    - Automatically detects schema changes
    - Scales processing based on data volume
    
    Returns:
        DataFrame: Streaming DataFrame with raw sales data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/sales")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "transaction_date DATE, amount DECIMAL(10,2)")
        .load("/mnt/landing/sales/")
    )


@dp.table(
    comment="Raw sales data with explicit schema (recommended for production)",
    table_properties={"quality": "bronze"}
)
def raw_sales_explicit_schema():
    """
    Ingest raw sales data with explicit schema definition.
    
    Explicit schemas are recommended for production because they:
    - Eliminate schema inference costs
    - Catch schema changes proactively
    - Provide data validation at ingestion
    - Enable better query optimization
    
    Returns:
        DataFrame: Streaming DataFrame with validated schema
    """
    from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
    
    # Define explicit schema
    sales_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("transaction_time", TimestampType(), False),
        StructField("store_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("quantity", DecimalType(10, 2), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("amount", DecimalType(10, 2), False),
        StructField("payment_method", StringType(), True),
        StructField("currency", StringType(), False)
    ])
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(sales_schema)  # Explicit schema - no inference needed
        .load("/mnt/landing/sales/")
    )


# Example: CSV ingestion variant
@dp.table(
    comment="Raw sales from CSV files",
    table_properties={"quality": "bronze"}
)
def raw_sales_csv():
    """
    Ingest sales data from CSV files.
    
    Demonstrates CSV-specific Auto Loader options.
    
    Returns:
        DataFrame: Streaming DataFrame from CSV source
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/sales_csv")
        .load("/mnt/landing/sales_csv/")
    )
