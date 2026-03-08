"""
# 01 - Introduction to Databricks Lakeflow

## Overview
Welcome to Mastering Databricks Lakeflow! This notebook introduces the fundamental concepts 
of declarative data engineering and the evolution from imperative notebooks to production-ready pipelines.

## What You'll Learn
- The paradigm shift from imperative to declarative programming
- The Lakeflow ecosystem: Connect, Pipelines, and Jobs
- Setting up your first declarative pipeline
- The new @dp decorator syntax

## Author
Ahmed Mahmoud - DataMindAI  
datamindaiwithhmed.com
"""

# ==============================================================================
# SECTION 1: The Paradigm Shift - Imperative vs Declarative
# ==============================================================================

"""
## Imperative vs Declarative

### The Old Way (Imperative) - "The Recipe"
In imperative programming, you tell the system HOW to do something, step by step:

```python
# Read data
df = spark.read.format("parquet").load("/path/to/data")

# Transform data
df_transformed = df.filter(col("status") == "active") \
                   .groupBy("customer_id") \
                   .agg(sum("amount").alias("total"))

# Write data
df_transformed.write.format("delta").mode("overwrite").save("/path/to/output")
```

**Problems:**
- You manage execution order manually
- You handle dependencies yourself
- You write retry logic
- You manage incremental processing
- Errors require custom handling

### The New Way (Declarative) - "The Menu"
In declarative programming, you tell the system WHAT you want, and it figures out HOW:

```python
from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Active customer totals"
)
def customer_totals():
    return spark.read.table("raw_customers") \
        .filter(col("status") == "active") \
        .groupBy("customer_id") \
        .agg(sum("amount").alias("total"))
```

**Benefits:**
- Engine manages execution automatically
- Dependencies are inferred from table references
- Automatic incremental processing
- Built-in retry and error handling
- Optimized compute resource allocation

**Key Principle:** Stop writing the recipe. Start ordering from the menu.
"""

# ==============================================================================
# SECTION 2: The Lakeflow Ecosystem
# ==============================================================================

"""
## The Lakeflow Ecosystem

Databricks Lakeflow provides a unified solution for the entire data engineering lifecycle:

### 1. Lakeflow Connect (Ingestion)
- Native connections to databases and applications
- Powered by Arcion technology with Change Data Capture (CDC)
- Supported sources:
  - Databases: SQL Server, MySQL, PostgreSQL, Oracle
  - Enterprise Apps: Salesforce, Workday, ServiceNow
  - Unstructured: SharePoint, PDFs, Excel

### 2. Lakeflow Pipelines (Transformation)
- Declarative Python files using @dp decorators
- Evolution of Delta Live Tables (DLT)
- Features:
  - Automatic orchestration
  - Incremental processing
  - Compute autoscaling
  - Data quality enforcement

### 3. Lakeflow Jobs (Orchestration)
- Reliable pipeline orchestration
- Supports triggers, branching, looping
- Full lineage tracking through Unity Catalog
"""

# ==============================================================================
# SECTION 3: Your First Lakeflow Pipeline
# ==============================================================================

"""
## Creating Your First Pipeline

Let's build a simple pipeline that:
1. Ingests raw data (Bronze)
2. Cleans and validates it (Silver)
3. Creates business metrics (Gold)
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# BRONZE LAYER: Raw Ingestion
@dp.table(
    comment="Raw sales data from S3",
    table_properties={"quality": "bronze"}
)
def bronze_sales():
    """
    Streaming table for continuous ingestion.
    Uses Auto Loader to automatically detect new files.
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/mnt/schemas/sales") \
        .load("/mnt/raw/sales")

# SILVER LAYER: Cleaned Data
@dp.materialized_view(
    comment="Cleaned and validated sales"
)
@dp.expect("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_date", "sale_date IS NOT NULL")
def silver_sales():
    """
    Materialized view with data quality rules.
    - Logs warning if amount <= 0
    - Drops rows with null sale_date
    """
    return spark.read.table("bronze_sales") \
        .select(
            "sale_id",
            "customer_id",
            "product_id",
            "amount",
            "sale_date"
        ) \
        .dropDuplicates(["sale_id"])

# GOLD LAYER: Business Metrics
@dp.materialized_view(
    comment="Daily sales summary for reporting"
)
def gold_daily_sales():
    """
    Aggregated business-ready dataset.
    Automatically refreshed when silver_sales updates.
    """
    return spark.read.table("silver_sales") \
        .groupBy("sale_date") \
        .agg(
            count("sale_id").alias("total_transactions"),
            sum("amount").alias("total_revenue"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("sale_date")

"""
## How This Pipeline Works

### Automatic Dependency Graph
The engine automatically infers that:
- silver_sales depends on bronze_sales
- gold_daily_sales depends on silver_sales

No need to explicitly define DAG dependencies!

### Incremental Processing
- bronze_sales: Processes only new files since last checkpoint
- silver_sales: Engine decides full refresh vs incremental
- gold_daily_sales: Updated only when upstream data changes

### Data Quality
- Expectations run automatically on every update
- Failed expectations are logged but don't block processing
- Use @dp.expect_or_fail for critical validations
"""

# ==============================================================================
# SECTION 4: Key Concepts Summary
# ==============================================================================

"""
## Key Takeaways

### 1. Decorator Syntax (@dp)
All pipeline definitions use decorators:
- `@dp.table` - For streaming/incremental ingestion
- `@dp.materialized_view` - For batch transformations
- `@dp.expect` - For data quality rules

### 2. File-Based Pipelines
- Use .py files, not notebooks
- Enables proper version control, CI/CD, and testing
- Follows software engineering best practices

### 3. Declarative Approach
- Declare WHAT you want (tables, views, expectations)
- Engine determines HOW to execute
- Automatic optimization and scaling

### 4. Medallion Architecture
- Bronze: Raw, append-only ingestion
- Silver: Cleaned, validated, deduplicated
- Gold: Business-ready aggregations

### 5. Unity Catalog Integration
- Pipelines are first-class governed objects
- Full lineage from source to dashboard
- Automated permission propagation

## Next Steps

1. Move to `02_streaming_tables_bronze.py` to learn about @dp.table in depth
2. Explore `03_materialized_views_silver.py` for transformation patterns
3. Master data quality in `04_data_quality_expectations.py`
4. Build complete architecture in `05_medallion_architecture_complete.py`

## Additional Resources

- Databricks Lakeflow Documentation: https://docs.databricks.com/workflows/delta-live-tables/
- DataMindAI Blog: https://datamindaiwithhmed.com
- YouTube Tutorial: Coming soon!
"""

# ==============================================================================
# TESTING THIS PIPELINE
# ==============================================================================

"""
## How to Run This Pipeline

### Option 1: Databricks UI
1. Go to Workflows → Delta Live Tables
2. Click "Create Pipeline"
3. Set source to this file path
4. Configure target schema and storage
5. Click "Start"

### Option 2: Databricks CLI
```bash
databricks pipelines create --settings pipeline_settings.json
databricks pipelines start --pipeline-id <pipeline_id>
```

### Option 3: API
```python
from databricks import pipelines

pipeline = pipelines.create(
    name="lakeflow_intro_pipeline",
    storage="/mnt/pipelines/intro",
    target="demo_catalog.intro_schema",
    libraries=[{"file": {"path": "/path/to/this/file.py"}}]
)
```

## Monitoring

Once running, you can:
- View lineage graph in the UI
- Check data quality metrics
- Monitor processing latency
- Inspect expectation failures
- Track compute resource usage
"""
