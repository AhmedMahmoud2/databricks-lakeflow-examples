"""
Bronze Layer: Raw Customer Data Ingestion

Demonstrates CDC (Change Data Capture) ingestion for customer data
using Lakeflow Connect patterns.
"""

from pyspark import pipelines as dp

@dp.table(
    comment="Raw customer data from operational database via CDC",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def raw_customers():
    """
    Ingest customer data using Change Data Capture (CDC).
    
    This pattern captures only changed records from the source database,
    making it far more efficient than full table dumps.
    
    CDC Benefits:
    - Captures only INSERT, UPDATE, DELETE operations
    - Reduces load on source systems
    - Provides near-real-time data
    - Maintains complete change history
    
    Returns:
        DataFrame: Streaming DataFrame with CDC data
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/customers")
        .load("/mnt/cdc/customers/")
    )


@dp.table(
    comment="Raw customer data from CRM system (Salesforce example)",
    table_properties={"quality": "bronze", "source": "salesforce"}
)
def raw_customers_salesforce():
    """
    Ingest customer data from Salesforce CRM.
    
    Demonstrates enterprise application integration using
    Lakeflow Connect for SaaS applications.
    
    Returns:
        DataFrame: Streaming DataFrame from Salesforce
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/customers_sfdc")
        .load("/mnt/landing/salesforce/accounts/")
    )
