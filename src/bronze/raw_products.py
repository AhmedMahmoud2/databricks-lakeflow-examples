"""
Bronze Layer: Raw Product Data Ingestion

Demonstrates batch ingestion patterns for slowly-changing dimension data.
"""

from pyspark import pipelines as dp

@dp.table(
    comment="Raw product catalog data",
    table_properties={"quality": "bronze"}
)
def raw_products():
    """
    Ingest product catalog data from JSON files.
    
    Products are typically slower-changing than transactions,
    but we still use streaming ingestion to capture updates
    as they arrive.
    
    Returns:
        DataFrame: Streaming DataFrame with product data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/products")
        .load("/mnt/landing/products/")
    )


@dp.table(
    comment="Raw product data from ERP system",
    table_properties={"quality": "bronze", "source": "erp"}
)
def raw_products_erp():
    """
    Ingest product master data from ERP system.
    
    Demonstrates integration with enterprise resource planning
    systems for product hierarchy and attributes.
    
    Returns:
        DataFrame: Streaming DataFrame from ERP
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/products_erp")
        .load("/mnt/landing/erp/products/")
    )
