# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 08 - Production Best Practices
# MAGIC
# MAGIC ## Overview
# MAGIC Production-ready patterns for enterprise Lakeflow deployments.
# MAGIC Learn from real-world experience to build robust, maintainable pipelines.
# MAGIC
# MAGIC ## Topics
# MAGIC - File organization
# MAGIC - Performance optimization
# MAGIC - Error handling
# MAGIC - Monitoring and alerting
# MAGIC - CI/CD integration
# MAGIC - Cost optimization
# MAGIC
# MAGIC ## Author
# MAGIC Ahmed Mahmoud - DataMindAI
# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==============================================================================
# BEST PRACTICE 1: Modular File Organization
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Recommended Project Structure
# MAGIC
# MAGIC ```
# MAGIC /pipelines/
# MAGIC   ├── config/
# MAGIC   │   ├── schemas.py          # Schema definitions
# MAGIC   │   └── settings.py         # Pipeline configurations
# MAGIC   ├── ingestion/
# MAGIC   │   ├── bronze_customers.py
# MAGIC   │   ├── bronze_orders.py
# MAGIC   │   └── bronze_products.py
# MAGIC   ├── transformation/
# MAGIC   │   ├── silver_customers.py
# MAGIC   │   ├── silver_orders.py
# MAGIC   │   └── enrichment.py
# MAGIC   ├── aggregation/
# MAGIC   │   ├── gold_metrics.py
# MAGIC   │   └── gold_reporting.py
# MAGIC   ├── quality/
# MAGIC   │   └── validation_rules.py
# MAGIC   └── main.py                 # Entry point
# MAGIC ```
# MAGIC
# MAGIC Benefits:
# MAGIC - Clear separation of concerns
# MAGIC - Easy to test individual modules
# MAGIC - Team members can work in parallel
# MAGIC - Reusable components
# COMMAND ----------
# ==============================================================================
# BEST PRACTICE 2: Explicit Schema Definitions
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Define Schemas Once, Reuse Everywhere
# MAGIC
# MAGIC schemas.py:
# COMMAND ----------
# Customer schema
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Order schema
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("order_date", DateType(), True)
])
# COMMAND ----------
# MAGIC %md
# MAGIC Use in pipelines:
# COMMAND ----------
@dp.table(comment="Customers with explicit schema")
def bronze_customers_typed():
    return spark.readStream \
        .schema(CUSTOMER_SCHEMA) \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/customers")

# ==============================================================================
# BEST PRACTICE 3: Configuration Management
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Externalize Configuration
# MAGIC
# MAGIC settings.py:
# COMMAND ----------
class PipelineConfig:
    # Source paths
    RAW_DATA_PATH = "/mnt/raw"
    CHECKPOINT_PATH = "/mnt/checkpoints"
    
    # Quality thresholds
    MIN_RECORDS_PER_BATCH = 100
    MAX_NULL_PERCENTAGE = 5.0
    
    # Performance tuning
    MAX_FILES_PER_TRIGGER = 1000
    MAX_BYTES_PER_TRIGGER = "1g"
    
    # Feature flags
    ENABLE_STRICT_VALIDATION = True
    ENABLE_AUTO_OPTIMIZE = True
# COMMAND ----------
# MAGIC %md
# MAGIC Use in pipelines:
# COMMAND ----------
@dp.table(
    comment="Configurable ingestion",
    table_properties={
        "delta.autoOptimize.optimizeWrite": str(PipelineConfig.ENABLE_AUTO_OPTIMIZE)
    }
)
def bronze_configurable():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.maxFilesPerTrigger", PipelineConfig.MAX_FILES_PER_TRIGGER) \
        .load(f"{PipelineConfig.RAW_DATA_PATH}/data")

# ==============================================================================
# BEST PRACTICE 4: Reusable Quality Functions
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Quality Rule Library
# MAGIC
# MAGIC validation_rules.py:
# COMMAND ----------
def email_validation_rule():
    return "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'"

def positive_amount_rule():
    return "amount > 0 AND amount < 1000000"

def future_date_check():
    return "date_field <= CURRENT_DATE()"
# COMMAND ----------
# MAGIC %md
# MAGIC Apply in pipelines:
# COMMAND ----------
@dp.materialized_view(comment="Validated with reusable rules")
@dp.expect("valid_email", email_validation_rule())
@dp.expect("valid_amount", positive_amount_rule())
def silver_validated():
    return spark.read.table("bronze_data")

# ==============================================================================
# BEST PRACTICE 5: Error Handling Patterns
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Robust Error Handling
# COMMAND ----------
@dp.table(comment="Ingestion with comprehensive error handling")
def bronze_robust():
    """
    Multi-layered error handling:
    1. Rescue columns for parse errors
    2. Corrupt record column for malformed JSON
    3. Metadata columns for debugging
    """
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/mnt/schemas/robust") \
        .option("rescuedDataColumn", "_rescued_data") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .load("/mnt/raw/data") \
        .select(
            "*",
            col("_metadata.file_path").alias("source_file"),
            current_timestamp().alias("ingestion_time")
        )

# Monitor errors
@dp.materialized_view(comment="Error monitoring dashboard")
def gold_error_monitoring():
    """
    Track and alert on errors.
    """
    return spark.read.table("bronze_robust") \
        .filter(
            col("_rescued_data").isNotNull() | 
            col("_corrupt_record").isNotNull()
        ) \
        .groupBy(
            to_date("ingestion_time").alias("date"),
            col("source_file")
        ) \
        .agg(
            count("*").alias("error_count"),
            countDistinct("_corrupt_record").alias("corrupt_records"),
            countDistinct("_rescued_data").alias("rescued_records")
        )

# ==============================================================================
# BEST PRACTICE 6: Performance Optimization
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Optimizing for Performance
# COMMAND ----------
@dp.table(
    comment="High-performance streaming table",
    table_properties={
        # Auto-optimize writes
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        
        # Enable liquid clustering (2026 feature)
        "delta.enableLiquidClustering": "true",
        "delta.liquidClustering.columns": "date,customer_id"
    }
)
def bronze_optimized():
    """
    Performance best practices:
    - Explicit schema (no inference)
    - Controlled batch sizes
    - Auto-optimize enabled
    - Liquid clustering for queries
    """
    return spark.readStream \
        .schema(ORDER_SCHEMA) \
        .format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.maxFilesPerTrigger", "500") \
        .option("cloudFiles.maxBytesPerTrigger", "512m") \
        .load("/mnt/raw/orders")

# ==============================================================================
# BEST PRACTICE 7: Testing Strategy
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Unit Testing Lakeflow Pipelines
# MAGIC
# MAGIC test_pipeline.py:
# MAGIC ```python
# MAGIC import pytest
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC @pytest.fixture
# MAGIC def spark():
# MAGIC     return SparkSession.builder.master("local[*]").getOrCreate()
# MAGIC
# MAGIC def test_customer_validation(spark):
# MAGIC     # Create test data
# MAGIC     test_data = [
# MAGIC         ("1", "valid@email.com", "US"),
# MAGIC         ("2", "invalid-email", "UK"),
# MAGIC         ("3", None, "CA")
# MAGIC     ]
# MAGIC
# MAGIC     df = spark.createDataFrame(test_data, ["id", "email", "country"])
# MAGIC
# MAGIC     # Apply validation logic
# MAGIC     validated = df.filter(col("email").rlike(".+@.+\\..+"))
# MAGIC
# MAGIC     # Assert expectations
# MAGIC     assert validated.count() == 1
# MAGIC     assert validated.first()["id"] == "1"
# MAGIC ```
# MAGIC
# MAGIC Integration testing:
# MAGIC ```bash
# MAGIC # Run pipeline in dev mode with test data
# MAGIC databricks pipelines create \\
# MAGIC     --settings test_settings.json \\
# MAGIC     --development true
# MAGIC
# MAGIC databricks pipelines start --pipeline-id <id>
# MAGIC
# MAGIC # Validate outputs
# MAGIC databricks sql execute \\
# MAGIC     "SELECT COUNT(*) FROM test_schema.bronze_customers"
# MAGIC ```
# COMMAND ----------
# ==============================================================================
# BEST PRACTICE 8: Monitoring and Observability
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Production Monitoring
# MAGIC
# MAGIC Create observability tables:
# COMMAND ----------
@dp.materialized_view(comment="Pipeline health metrics")
def monitoring_pipeline_health():
    """
    Track pipeline performance over time.
    """
    return spark.read.table("event_log") \
        .filter(col("event_type") == "flow_progress") \
        .groupBy("flow_name", to_date("timestamp").alias("date")) \
        .agg(
            count("*").alias("runs"),
            avg("num_output_rows").alias("avg_rows"),
            max("data_quality.expectations.passed_records").alias("max_passed"),
            max("data_quality.expectations.failed_records").alias("max_failed")
        )

# ==============================================================================
# KEY TAKEAWAYS
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Production Checklist
# MAGIC
# MAGIC ### Architecture
# MAGIC - [ ] Modular file organization
# MAGIC - [ ] Separate config from code
# MAGIC - [ ] Reusable schemas and functions
# MAGIC - [ ] Clear layer separation (Bronze/Silver/Gold)
# MAGIC
# MAGIC ### Performance
# MAGIC - [ ] Explicit schemas everywhere
# MAGIC - [ ] Controlled batch sizes (maxFilesPerTrigger)
# MAGIC - [ ] Auto-optimize enabled
# MAGIC - [ ] Liquid clustering configured
# MAGIC - [ ] Query optimization with Z-ORDER
# MAGIC
# MAGIC ### Quality
# MAGIC - [ ] Comprehensive expectations
# MAGIC - [ ] Error handling with rescue columns
# MAGIC - [ ] Monitoring dashboard for issues
# MAGIC - [ ] Alerting on quality violations
# MAGIC
# MAGIC ### Operations
# MAGIC - [ ] CI/CD pipeline configured
# MAGIC - [ ] Unit and integration tests
# MAGIC - [ ] Monitoring and alerting
# MAGIC - [ ] Documentation and runbooks
# MAGIC - [ ] Backup and recovery procedures
# MAGIC
# MAGIC ### Cost Optimization
# MAGIC - [ ] Serverless compute enabled
# MAGIC - [ ] Appropriate cluster sizing
# MAGIC - [ ] VACUUM old data regularly
# MAGIC - [ ] Monitor compute usage
# MAGIC - [ ] Review storage costs
# MAGIC
# MAGIC ## Serverless Best Practice
# MAGIC
# MAGIC ```python
# MAGIC # Pipeline configuration for serverless
# MAGIC {
# MAGIC   "name": "production_pipeline",
# MAGIC   "channel": "PREVIEW",  # Enable serverless
# MAGIC   "serverless": true,
# MAGIC   "clusters": [],  # No cluster config needed
# MAGIC   "libraries": [{"file": {"path": "/path/to/pipeline.py"}}],
# MAGIC   "target": "main.production",
# MAGIC   "storage": "/mnt/pipelines/production"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ## CI/CD Integration
# MAGIC
# MAGIC ```yaml
# MAGIC # .github/workflows/deploy.yml
# MAGIC name: Deploy Pipeline
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v2
# MAGIC
# MAGIC       - name: Deploy to Databricks
# MAGIC         run: |
# MAGIC           databricks pipelines update \\
# MAGIC             --pipeline-id $PIPELINE_ID \\
# MAGIC             --settings pipeline_settings.json
# MAGIC
# MAGIC           databricks pipelines start \\
# MAGIC             --pipeline-id $PIPELINE_ID
# MAGIC ```
# MAGIC
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've completed the Mastering Databricks Lakeflow course!
# MAGIC
# MAGIC You now know:
# MAGIC ✓ Declarative pipeline patterns
# MAGIC ✓ Streaming and batch processing
# MAGIC ✓ Data quality enforcement
# MAGIC ✓ Medallion architecture
# MAGIC ✓ CDC and SCD Type 2
# MAGIC ✓ Migration strategies
# MAGIC ✓ Production best practices
# MAGIC
# MAGIC ## Resources
# MAGIC
# MAGIC - DataMindAI Blog: https://datamindaiwithhmed.com
# MAGIC - Databricks Documentation: https://docs.databricks.com
# MAGIC - GitHub Repo: https://github.com/yourusername/databricks-lakeflow-examples
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. Build your first production pipeline
# MAGIC 2. Join the DataMindAI community
# MAGIC 3. Share your learnings
# MAGIC 4. Contribute to this repo!
# MAGIC
# MAGIC ---
# MAGIC © 2026 DataMindAI | Turn Your Data Into Decision-Ready Intelligence
