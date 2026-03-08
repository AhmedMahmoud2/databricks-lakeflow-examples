"""
# 08 - Production Best Practices

## Overview
Production-ready patterns for enterprise Lakeflow deployments.
Learn from real-world experience to build robust, maintainable pipelines.

## Topics
- File organization
- Performance optimization
- Error handling
- Monitoring and alerting
- CI/CD integration
- Cost optimization

## Author
Ahmed Mahmoud - DataMindAI
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==============================================================================
# BEST PRACTICE 1: Modular File Organization
# ==============================================================================

"""
## Recommended Project Structure

```
/pipelines/
  ├── config/
  │   ├── schemas.py          # Schema definitions
  │   └── settings.py         # Pipeline configurations
  ├── ingestion/
  │   ├── bronze_customers.py
  │   ├── bronze_orders.py
  │   └── bronze_products.py
  ├── transformation/
  │   ├── silver_customers.py
  │   ├── silver_orders.py
  │   └── enrichment.py
  ├── aggregation/
  │   ├── gold_metrics.py
  │   └── gold_reporting.py
  ├── quality/
  │   └── validation_rules.py
  └── main.py                 # Entry point
```

Benefits:
- Clear separation of concerns
- Easy to test individual modules
- Team members can work in parallel
- Reusable components
"""

# ==============================================================================
# BEST PRACTICE 2: Explicit Schema Definitions
# ==============================================================================

"""
## Define Schemas Once, Reuse Everywhere

schemas.py:
"""

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

"""
Use in pipelines:
"""

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

"""
## Externalize Configuration

settings.py:
"""

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

"""
Use in pipelines:
"""

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

"""
## Create Quality Rule Library

validation_rules.py:
"""

def email_validation_rule():
    return "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'"

def positive_amount_rule():
    return "amount > 0 AND amount < 1000000"

def future_date_check():
    return "date_field <= CURRENT_DATE()"

"""
Apply in pipelines:
"""

@dp.materialized_view(comment="Validated with reusable rules")
@dp.expect("valid_email", email_validation_rule())
@dp.expect("valid_amount", positive_amount_rule())
def silver_validated():
    return spark.read.table("bronze_data")

# ==============================================================================
# BEST PRACTICE 5: Error Handling Patterns
# ==============================================================================

"""
## Robust Error Handling
"""

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

"""
## Optimizing for Performance
"""

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

"""
## Unit Testing Lakeflow Pipelines

test_pipeline.py:
```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[*]").getOrCreate()

def test_customer_validation(spark):
    # Create test data
    test_data = [
        ("1", "valid@email.com", "US"),
        ("2", "invalid-email", "UK"),
        ("3", None, "CA")
    ]
    
    df = spark.createDataFrame(test_data, ["id", "email", "country"])
    
    # Apply validation logic
    validated = df.filter(col("email").rlike(".+@.+\\..+"))
    
    # Assert expectations
    assert validated.count() == 1
    assert validated.first()["id"] == "1"
```

Integration testing:
```bash
# Run pipeline in dev mode with test data
databricks pipelines create \\
    --settings test_settings.json \\
    --development true

databricks pipelines start --pipeline-id <id>

# Validate outputs
databricks sql execute \\
    "SELECT COUNT(*) FROM test_schema.bronze_customers"
```
"""

# ==============================================================================
# BEST PRACTICE 8: Monitoring and Observability
# ==============================================================================

"""
## Production Monitoring

Create observability tables:
"""

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

"""
## Production Checklist

### Architecture
- [ ] Modular file organization
- [ ] Separate config from code
- [ ] Reusable schemas and functions
- [ ] Clear layer separation (Bronze/Silver/Gold)

### Performance
- [ ] Explicit schemas everywhere
- [ ] Controlled batch sizes (maxFilesPerTrigger)
- [ ] Auto-optimize enabled
- [ ] Liquid clustering configured
- [ ] Query optimization with Z-ORDER

### Quality
- [ ] Comprehensive expectations
- [ ] Error handling with rescue columns
- [ ] Monitoring dashboard for issues
- [ ] Alerting on quality violations

### Operations
- [ ] CI/CD pipeline configured
- [ ] Unit and integration tests
- [ ] Monitoring and alerting
- [ ] Documentation and runbooks
- [ ] Backup and recovery procedures

### Cost Optimization
- [ ] Serverless compute enabled
- [ ] Appropriate cluster sizing
- [ ] VACUUM old data regularly
- [ ] Monitor compute usage
- [ ] Review storage costs

## Serverless Best Practice

```python
# Pipeline configuration for serverless
{
  "name": "production_pipeline",
  "channel": "PREVIEW",  # Enable serverless
  "serverless": true,
  "clusters": [],  # No cluster config needed
  "libraries": [{"file": {"path": "/path/to/pipeline.py"}}],
  "target": "main.production",
  "storage": "/mnt/pipelines/production"
}
```

## CI/CD Integration

```yaml
# .github/workflows/deploy.yml
name: Deploy Pipeline

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Deploy to Databricks
        run: |
          databricks pipelines update \\
            --pipeline-id $PIPELINE_ID \\
            --settings pipeline_settings.json
          
          databricks pipelines start \\
            --pipeline-id $PIPELINE_ID
```

## Congratulations!

You've completed the Mastering Databricks Lakeflow course!

You now know:
✓ Declarative pipeline patterns
✓ Streaming and batch processing
✓ Data quality enforcement
✓ Medallion architecture
✓ CDC and SCD Type 2
✓ Migration strategies
✓ Production best practices

## Resources

- DataMindAI Blog: https://datamindaiwithhmed.com
- Databricks Documentation: https://docs.databricks.com
- GitHub Repo: https://github.com/yourusername/databricks-lakeflow-examples

## Next Steps

1. Build your first production pipeline
2. Join the DataMindAI community
3. Share your learnings
4. Contribute to this repo!

---
© 2026 DataMindAI | Turn Your Data Into Decision-Ready Intelligence
"""
