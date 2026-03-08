# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01 - Introduction to Databricks Lakeflow
# MAGIC
# MAGIC ## Overview
# MAGIC Welcome to Mastering Databricks Lakeflow! This notebook introduces the fundamental concepts 
# MAGIC of declarative data engineering and the evolution from imperative notebooks to production-ready pipelines.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - The paradigm shift from imperative to declarative programming
# MAGIC - The Lakeflow ecosystem: Connect, Pipelines, and Jobs
# MAGIC - Setting up your first declarative pipeline
# MAGIC - The new @dp decorator syntax
# MAGIC
# MAGIC ## Author
# MAGIC Ahmed Mahmoud - DataMindAI  
# MAGIC datamindaiwithhmed.com
# COMMAND ----------
# ==============================================================================
# SECTION 1: The Paradigm Shift - Imperative vs Declarative
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Imperative vs Declarative
# MAGIC
# MAGIC ### The Old Way (Imperative) - "The Recipe"
# MAGIC In imperative programming, you tell the system HOW to do something, step by step:
# MAGIC
# MAGIC ```python
# MAGIC # Read data
# MAGIC df = spark.read.format("parquet").load("/path/to/data")
# MAGIC
# MAGIC # Transform data
# MAGIC df_transformed = df.filter(col("status") == "active") \
# MAGIC                    .groupBy("customer_id") \
# MAGIC                    .agg(sum("amount").alias("total"))
# MAGIC
# MAGIC # Write data
# MAGIC df_transformed.write.format("delta").mode("overwrite").save("/path/to/output")
# MAGIC ```
# MAGIC
# MAGIC **Problems:**
# MAGIC - You manage execution order manually
# MAGIC - You handle dependencies yourself
# MAGIC - You write retry logic
# MAGIC - You manage incremental processing
# MAGIC - Errors require custom handling
# MAGIC
# MAGIC ### The New Way (Declarative) - "The Menu"
# MAGIC In declarative programming, you tell the system WHAT you want, and it figures out HOW:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC @dp.materialized_view(
# MAGIC     comment="Active customer totals"
# MAGIC )
# MAGIC def customer_totals():
# MAGIC     return spark.read.table("raw_customers") \
# MAGIC         .filter(col("status") == "active") \
# MAGIC         .groupBy("customer_id") \
# MAGIC         .agg(sum("amount").alias("total"))
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Engine manages execution automatically
# MAGIC - Dependencies are inferred from table references
# MAGIC - Automatic incremental processing
# MAGIC - Built-in retry and error handling
# MAGIC - Optimized compute resource allocation
# MAGIC
# MAGIC **Key Principle:** Stop writing the recipe. Start ordering from the menu.
# COMMAND ----------
# ==============================================================================
# SECTION 2: The Lakeflow Ecosystem
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## The Lakeflow Ecosystem
# MAGIC
# MAGIC Databricks Lakeflow provides a unified solution for the entire data engineering lifecycle:
# MAGIC
# MAGIC ### 1. Lakeflow Connect (Ingestion)
# MAGIC - Native connections to databases and applications
# MAGIC - Powered by Arcion technology with Change Data Capture (CDC)
# MAGIC - Supported sources:
# MAGIC   - Databases: SQL Server, MySQL, PostgreSQL, Oracle
# MAGIC   - Enterprise Apps: Salesforce, Workday, ServiceNow
# MAGIC   - Unstructured: SharePoint, PDFs, Excel
# MAGIC
# MAGIC ### 2. Lakeflow Pipelines (Transformation)
# MAGIC - Declarative Python files using @dp decorators
# MAGIC - Evolution of Delta Live Tables (DLT)
# MAGIC - Features:
# MAGIC   - Automatic orchestration
# MAGIC   - Incremental processing
# MAGIC   - Compute autoscaling
# MAGIC   - Data quality enforcement
# MAGIC
# MAGIC ### 3. Lakeflow Jobs (Orchestration)
# MAGIC - Reliable pipeline orchestration
# MAGIC - Supports triggers, branching, looping
# MAGIC - Full lineage tracking through Unity Catalog
# COMMAND ----------
# ==============================================================================
# SECTION 3: Your First Lakeflow Pipeline
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Creating Your First Pipeline
# MAGIC
# MAGIC Let's build a simple pipeline that:
# MAGIC 1. Ingests raw data (Bronze)
# MAGIC 2. Cleans and validates it (Silver)
# MAGIC 3. Creates business metrics (Gold)
# COMMAND ----------
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
# COMMAND ----------
# MAGIC %md
# MAGIC ## How This Pipeline Works
# MAGIC
# MAGIC ### Automatic Dependency Graph
# MAGIC The engine automatically infers that:
# MAGIC - silver_sales depends on bronze_sales
# MAGIC - gold_daily_sales depends on silver_sales
# MAGIC
# MAGIC No need to explicitly define DAG dependencies!
# MAGIC
# MAGIC ### Incremental Processing
# MAGIC - bronze_sales: Processes only new files since last checkpoint
# MAGIC - silver_sales: Engine decides full refresh vs incremental
# MAGIC - gold_daily_sales: Updated only when upstream data changes
# MAGIC
# MAGIC ### Data Quality
# MAGIC - Expectations run automatically on every update
# MAGIC - Failed expectations are logged but don't block processing
# MAGIC - Use @dp.expect_or_fail for critical validations
# COMMAND ----------
# ==============================================================================
# SECTION 4: Key Concepts Summary
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### 1. Decorator Syntax (@dp)
# MAGIC All pipeline definitions use decorators:
# MAGIC - `@dp.table` - For streaming/incremental ingestion
# MAGIC - `@dp.materialized_view` - For batch transformations
# MAGIC - `@dp.expect` - For data quality rules
# MAGIC
# MAGIC ### 2. File-Based Pipelines
# MAGIC - Use .py files, not notebooks
# MAGIC - Enables proper version control, CI/CD, and testing
# MAGIC - Follows software engineering best practices
# MAGIC
# MAGIC ### 3. Declarative Approach
# MAGIC - Declare WHAT you want (tables, views, expectations)
# MAGIC - Engine determines HOW to execute
# MAGIC - Automatic optimization and scaling
# MAGIC
# MAGIC ### 4. Medallion Architecture
# MAGIC - Bronze: Raw, append-only ingestion
# MAGIC - Silver: Cleaned, validated, deduplicated
# MAGIC - Gold: Business-ready aggregations
# MAGIC
# MAGIC ### 5. Unity Catalog Integration
# MAGIC - Pipelines are first-class governed objects
# MAGIC - Full lineage from source to dashboard
# MAGIC - Automated permission propagation
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. Move to `02_streaming_tables_bronze.py` to learn about @dp.table in depth
# MAGIC 2. Explore `03_materialized_views_silver.py` for transformation patterns
# MAGIC 3. Master data quality in `04_data_quality_expectations.py`
# MAGIC 4. Build complete architecture in `05_medallion_architecture_complete.py`
# MAGIC
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - Databricks Lakeflow Documentation: https://docs.databricks.com/workflows/delta-live-tables/
# MAGIC - DataMindAI Blog: https://datamindaiwithhmed.com
# MAGIC - YouTube Tutorial: Coming soon!
# COMMAND ----------
# ==============================================================================
# TESTING THIS PIPELINE
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## How to Run This Pipeline
# MAGIC
# MAGIC ### Option 1: Databricks UI
# MAGIC 1. Go to Workflows → Delta Live Tables
# MAGIC 2. Click "Create Pipeline"
# MAGIC 3. Set source to this file path
# MAGIC 4. Configure target schema and storage
# MAGIC 5. Click "Start"
# MAGIC
# MAGIC ### Option 2: Databricks CLI
# MAGIC ```bash
# MAGIC databricks pipelines create --settings pipeline_settings.json
# MAGIC databricks pipelines start --pipeline-id <pipeline_id>
# MAGIC ```
# MAGIC
# MAGIC ### Option 3: API
# MAGIC ```python
# MAGIC from databricks import pipelines
# MAGIC
# MAGIC pipeline = pipelines.create(
# MAGIC     name="lakeflow_intro_pipeline",
# MAGIC     storage="/mnt/pipelines/intro",
# MAGIC     target="demo_catalog.intro_schema",
# MAGIC     libraries=[{"file": {"path": "/path/to/this/file.py"}}]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ## Monitoring
# MAGIC
# MAGIC Once running, you can:
# MAGIC - View lineage graph in the UI
# MAGIC - Check data quality metrics
# MAGIC - Monitor processing latency
# MAGIC - Inspect expectation failures
# MAGIC - Track compute resource usage
