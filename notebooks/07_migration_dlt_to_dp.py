# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 07 - Migration Guide: DLT → Lakeflow
# MAGIC
# MAGIC ## Overview
# MAGIC Step-by-step guide to migrate from Delta Live Tables (@dlt) to Lakeflow Pipelines (@dp).
# MAGIC
# MAGIC ## Migration Strategy
# MAGIC 1. Convert imports
# MAGIC 2. Update decorators
# MAGIC 3. Move from notebooks to .py files
# MAGIC 4. Test in development
# MAGIC 5. Deploy to production
# MAGIC
# MAGIC ## Author
# MAGIC Ahmed Mahmoud - DataMindAI
# COMMAND ----------
# ==============================================================================
# MIGRATION REFERENCE TABLE
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Syntax Migration Reference
# MAGIC
# MAGIC | Old (DLT) | New (Lakeflow @dp) | Notes |
# MAGIC |-----------|-------------------|-------|
# MAGIC | `import dlt` | `from pyspark import pipelines as dp` | New import |
# MAGIC | `@dlt.table(streaming=True)` | `@dp.table` | Streaming table |
# MAGIC | `@dlt.table()` | `@dp.materialized_view` | Batch/materialized |
# MAGIC | `@dlt.expect()` | `@dp.expect()` | Quality check |
# MAGIC | `@dlt.expect_or_drop()` | `@dp.expect_or_drop()` | Same |
# MAGIC | `@dlt.expect_or_fail()` | `@dp.expect_or_fail()` | Same |
# MAGIC | `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` | CDC function |
# MAGIC | `dlt.read()` | `spark.read.table()` | Standard read |
# MAGIC | `dlt.read_stream()` | `spark.readStream.table()` | Streaming read |
# COMMAND ----------
# ==============================================================================
# EXAMPLE 1: Basic Table Migration
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC BEFORE (DLT):
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name="customers",
# MAGIC     comment="Customer data",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC def customers():
# MAGIC     return spark.read.table("raw_customers")
# MAGIC ```
# MAGIC
# MAGIC AFTER (Lakeflow):
# COMMAND ----------
from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Customer data",
    table_properties={"quality": "silver"}
)
def customers():
    return spark.read.table("raw_customers")
# COMMAND ----------
# MAGIC %md
# MAGIC Changes:
# MAGIC - Import changed to dp
# MAGIC - @dlt.table() → @dp.materialized_view
# MAGIC - name parameter removed (function name becomes table name)
# MAGIC - Everything else stays the same
# COMMAND ----------
# ==============================================================================
# EXAMPLE 2: Streaming Table Migration
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC BEFORE (DLT):
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     name="raw_orders",
# MAGIC     streaming=True,
# MAGIC     comment="Raw orders from S3"
# MAGIC )
# MAGIC def raw_orders():
# MAGIC     return spark.readStream.format("cloudFiles") \
# MAGIC         .option("cloudFiles.format", "json") \
# MAGIC         .load("/mnt/raw/orders")
# MAGIC ```
# MAGIC
# MAGIC AFTER (Lakeflow):
# COMMAND ----------
@dp.table(
    comment="Raw orders from S3"
)
def raw_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/orders")
# COMMAND ----------
# MAGIC %md
# MAGIC Changes:
# MAGIC - @dlt.table(streaming=True) → @dp.table
# MAGIC - streaming parameter removed (implicit in @dp.table)
# MAGIC - name parameter removed
# COMMAND ----------
# ==============================================================================
# EXAMPLE 3: Expectations Migration
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC BEFORE (DLT):
# MAGIC ```python
# MAGIC @dlt.table()
# MAGIC @dlt.expect("valid_email", "email IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_age", "age >= 18")
# MAGIC def clean_customers():
# MAGIC     return dlt.read("raw_customers").select("*")
# MAGIC ```
# MAGIC
# MAGIC AFTER (Lakeflow):
# COMMAND ----------
@dp.materialized_view()
@dp.expect("valid_email", "email IS NOT NULL")
@dp.expect_or_drop("valid_age", "age >= 18")
def clean_customers():
    return spark.read.table("raw_customers").select("*")
# COMMAND ----------
# MAGIC %md
# MAGIC Changes:
# MAGIC - @dlt.table() → @dp.materialized_view()
# MAGIC - @dlt.expect → @dp.expect (same API!)
# MAGIC - dlt.read() → spark.read.table()
# COMMAND ----------
# ==============================================================================
# EXAMPLE 4: CDC Migration
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC BEFORE (DLT):
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="customers_scd2",
# MAGIC     source="raw_customer_changes",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=2
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC AFTER (Lakeflow):
# COMMAND ----------
dp.create_auto_cdc_flow(
    target="customers_scd2",
    source="raw_customer_changes",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
# COMMAND ----------
# MAGIC %md
# MAGIC Changes:
# MAGIC - dlt.apply_changes() → dp.create_auto_cdc_flow()
# MAGIC - All parameters remain the same
# MAGIC - Function name more descriptive
# COMMAND ----------
# ==============================================================================
# MIGRATION CHECKLIST
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Step-by-Step Migration
# MAGIC
# MAGIC ### Step 1: Convert Notebooks to .py Files ✓
# MAGIC 1. Create `/transformations` directory
# MAGIC 2. Copy notebook cells to .py file
# MAGIC 3. Remove magic commands (%sql, %md, etc.)
# MAGIC 4. Keep only Python code and docstrings
# MAGIC
# MAGIC ### Step 2: Update Imports ✓
# MAGIC ```python
# MAGIC # Find and replace
# MAGIC # OLD: import dlt
# MAGIC # NEW: from pyspark import pipelines as dp
# MAGIC ```
# MAGIC
# MAGIC ### Step 3: Update Decorators ✓
# MAGIC - @dlt.table(streaming=True) → @dp.table
# MAGIC - @dlt.table() → @dp.materialized_view
# MAGIC - Remove 'name' parameters (use function name)
# MAGIC
# MAGIC ### Step 4: Update Read Statements ✓
# MAGIC - dlt.read("table") → spark.read.table("table")
# MAGIC - dlt.read_stream("table") → spark.readStream.table("table")
# MAGIC
# MAGIC ### Step 5: Explicit Schemas ✓
# MAGIC Add schema definitions where inference was used:
# MAGIC ```python
# MAGIC schema = StructType([
# MAGIC     StructField("id", StringType(), False),
# MAGIC     StructField("name", StringType(), True)
# MAGIC ])
# MAGIC
# MAGIC @dp.table()
# MAGIC def my_table():
# MAGIC     return spark.readStream.schema(schema).format("cloudFiles") \
# MAGIC         .option("cloudFiles.format", "json") \
# MAGIC         .load("/path")
# MAGIC ```
# MAGIC
# MAGIC ### Step 6: Separate Concerns ✓
# MAGIC Modularize into separate files:
# MAGIC - `bronze_layer.py` - Raw ingestion
# MAGIC - `silver_layer.py` - Cleansing
# MAGIC - `gold_layer.py` - Aggregations
# MAGIC
# MAGIC ### Step 7: Test in Development ✓
# MAGIC 1. Create test pipeline in dev workspace
# MAGIC 2. Run with small dataset
# MAGIC 3. Validate outputs
# MAGIC 4. Check data quality metrics
# MAGIC
# MAGIC ### Step 8: Deploy to Production ✓
# MAGIC 1. Update pipeline configuration
# MAGIC 2. Point to new .py file(s)
# MAGIC 3. Run in full mode first
# MAGIC 4. Switch to incremental mode
# MAGIC 5. Monitor for issues
# MAGIC
# MAGIC ## Common Gotchas
# MAGIC
# MAGIC ### ❌ Using .collect() or .count()
# MAGIC ```python
# MAGIC # WRONG
# MAGIC @dp.materialized_view()
# MAGIC def bad_table():
# MAGIC     df = spark.read.table("source")
# MAGIC     count = df.count()  # ❌ Don't do this
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC ### ✓ Correct Approach
# MAGIC ```python
# MAGIC # RIGHT
# MAGIC @dp.materialized_view()
# MAGIC def good_table():
# MAGIC     return spark.read.table("source")  # ✓ Just return DataFrame
# MAGIC ```
# MAGIC
# MAGIC ### ❌ Mixing Streaming and Batch
# MAGIC ```python
# MAGIC # WRONG
# MAGIC @dp.table()  # Streaming decorator
# MAGIC def mixed_table():
# MAGIC     return spark.read.table("source")  # ❌ Batch read
# MAGIC ```
# MAGIC
# MAGIC ### ✓ Match Read Type to Decorator
# MAGIC ```python
# MAGIC # RIGHT
# MAGIC @dp.table()  # Streaming decorator
# MAGIC def streaming_table():
# MAGIC     return spark.readStream.table("source")  # ✓ Streaming read
# MAGIC
# MAGIC @dp.materialized_view()  # Batch decorator
# MAGIC def batch_table():
# MAGIC     return spark.read.table("source")  # ✓ Batch read
# MAGIC ```
# MAGIC
# MAGIC ## Testing Your Migration
# MAGIC
# MAGIC ```python
# MAGIC # Create test pipeline
# MAGIC from databricks import pipelines
# MAGIC
# MAGIC test_pipeline = pipelines.create(
# MAGIC     name="migration_test",
# MAGIC     storage="/dbfs/pipelines/test/",
# MAGIC     target="test_schema",
# MAGIC     libraries=[{"file": {"path": "/path/to/migrated.py"}}],
# MAGIC     development=True  # Run in dev mode first
# MAGIC )
# MAGIC
# MAGIC # Start and monitor
# MAGIC pipelines.start(test_pipeline.pipeline_id)
# MAGIC ```
# MAGIC
# MAGIC ## Rollback Plan
# MAGIC
# MAGIC If issues arise:
# MAGIC 1. Keep old DLT pipeline available
# MAGIC 2. Can switch back by changing source path
# MAGIC 3. Delta tables are forward/backward compatible
# MAGIC 4. Test thoroughly before decommissioning old pipeline
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC See `08_best_practices_production.py` for production-ready
# MAGIC patterns and optimization strategies.
