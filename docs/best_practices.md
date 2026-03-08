# Databricks Lakeflow Best Practices (2026 Edition)

Production-ready patterns for building reliable data pipelines.

## 1. File Organization

### Directory Structure
```
src/
├── bronze/              # Raw ingestion layer
│   ├── raw_sales.py
│   ├── raw_customers.py
│   └── raw_products.py
├── silver/              # Cleaning and validation
│   ├── clean_sales.py
│   ├── clean_customers.py
│   └── clean_products.py
└── gold/                # Business aggregations
    ├── daily_summary.py
    ├── customer_metrics.py
    └── product_performance.py
```

### File Naming Conventions
- Use descriptive, lowercase names
- Prefix with layer: `raw_`, `clean_`, `agg_`
- One business entity per file
- Max 500 lines per file

## 2. Schema Management

### Always Use Explicit Schemas in Production

❌ **Don't:**
```python
@dp.table
def raw_sales():
    return spark.readStream.format("cloudFiles") \\
        .option("cloudFiles.inferColumnTypes", "true") \\
        .load("/sales")
```

✅ **Do:**
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", DecimalType(10,2), False),
    StructField("date", DateType(), False)
])

@dp.table
def raw_sales():
    return spark.readStream.format("cloudFiles") \\
        .schema(schema) \\
        .load("/sales")
```

**Why?**
- No schema inference costs
- Catch schema changes proactively
- Better query optimization
- Data validation at source

## 3. Data Quality Strategy

### Three-Tier System

```python
@dp.materialized_view
@dp.expect("monitor", "condition")         # Tier 1: Monitor
@dp.expect_or_drop("clean", "condition")   # Tier 2: Clean
@dp.expect_or_fail("critical", "condition") # Tier 3: Critical
def my_table():
    return spark.read.table("source")
```

### When to Use Each Tier

**Tier 1 - @dp.expect (Monitor):**
- Completeness checks
- Data freshness monitoring
- Anomaly detection
- Non-critical validations

**Tier 2 - @dp.expect_or_drop (Clean):**
- Invalid email formats
- Out-of-range values
- Malformed records
- Duplicate removal

**Tier 3 - @dp.expect_or_fail (Critical):**
- Financial reconciliation
- Regulatory compliance
- System integration points
- Master data integrity

## 4. Medallion Architecture

### Bronze Layer (Raw)
```python
@dp.table(
    comment="Raw data - no transformations",
    table_properties={"quality": "bronze"}
)
def bronze_sales():
    return spark.readStream.format("cloudFiles").load("/raw")
```

**Bronze Layer Rules:**
- Append-only, never update
- Keep complete history
- Minimal transformations
- Preserve source structure

### Silver Layer (Clean)
```python
@dp.materialized_view(
    comment="Cleaned and validated"
)
@dp.expect_or_drop("valid", "condition")
def silver_sales():
    return (
        spark.read.table("bronze_sales")
        .dropDuplicates()
        .filter(col("is_valid"))
    )
```

**Silver Layer Rules:**
- Deduplicate records
- Standardize formats
- Validate data quality
- Enrich with references

### Gold Layer (Business)
```python
@dp.materialized_view(
    comment="Business-ready metrics"
)
def gold_daily_sales():
    return (
        spark.read.table("silver_sales")
        .groupBy("date").agg(sum("amount"))
    )
```

**Gold Layer Rules:**
- Consumption-optimized
- Business terminology
- Pre-aggregated metrics
- Denormalized for performance

## 5. Performance Optimization

### Enable Auto-Optimize
```python
@dp.table(
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    }
)
```

### Partition Strategy
```python
@dp.materialized_view
def partitioned_table():
    return (
        spark.read.table("source")
        .repartition("date", "region")
    )
```

### Broadcast Joins
```python
from pyspark.sql.functions import broadcast

@dp.materialized_view
def joined_data():
    large = spark.read.table("fact_table")
    small = spark.read.table("dim_table")
    return large.join(broadcast(small), "key")
```

## 6. Error Handling

### Graceful Degradation
```python
@dp.materialized_view
@dp.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0")
def clean_sales():
    return (
        spark.read.table("raw_sales")
        .withColumn("amount", 
            coalesce(col("amount"), lit(0))  # Default for nulls
        )
    )
```

### Add Processing Metadata
```python
@dp.materialized_view
def tracked_table():
    return (
        spark.read.table("source")
        .withColumn("processed_at", current_timestamp())
        .withColumn("pipeline_id", lit("sales_pipeline_v2"))
    )
```

## 7. Testing Strategy

### Unit Tests for Business Logic
```python
# test_transformations.py
def test_sales_aggregation():
    # Create test data
    test_df = spark.createDataFrame([
        ("2024-01-01", "A", 100),
        ("2024-01-01", "A", 200)
    ], ["date", "store", "amount"])
    
    # Apply transformation logic
    result = test_df.groupBy("date", "store").agg(sum("amount"))
    
    # Assert expected outcome
    assert result.count() == 1
    assert result.first()["sum(amount)"] == 300
```

### Integration Tests
```python
# Run pipeline with test data
# Validate output matches expected
# Check data quality metrics
```

## 8. Monitoring & Observability

### Add Quality Metrics
```python
@dp.materialized_view
def monitored_table():
    base = spark.read.table("source")
    
    return base.withColumn(
        "quality_score",
        when(col("customer_id").isNull(), 0.5)
        .when(col("email").isNull(), 0.8)
        .otherwise(1.0)
    )
```

### Track Lineage
- Use Unity Catalog for automatic lineage
- Document dependencies in comments
- Maintain data dictionary

## 9. Documentation

### Inline Comments
```python
@dp.materialized_view(
    comment="Daily sales aggregated by store and category. "
            "Used by: Sales Dashboard, Executive Reports. "
            "SLA: Updated hourly. "
            "Owner: data-team@company.com"
)
def daily_sales_summary():
    return spark.read.table("clean_sales").groupBy(...)
```

### README per Layer
Document each layer's:
- Purpose and scope
- Data sources
- Refresh schedule
- Dependencies
- Contact information

## 10. Security & Governance

### Unity Catalog Integration
```python
# Tables automatically registered in Unity Catalog
@dp.materialized_view
def customer_data():
    return spark.read.table("customers")
```

### Row-Level Security
```python
@dp.materialized_view
def filtered_by_region():
    return (
        spark.read.table("source")
        .filter(col("region") == current_user_region())
    )
```

## Anti-Patterns to Avoid

❌ **Don't:**
1. Mix bronze/silver/gold in one file
2. Use notebooks for production pipelines
3. Skip schema definitions
4. Ignore data quality checks
5. Create circular dependencies
6. Use action methods in definitions
7. Hard-code credentials or paths
8. Skip documentation

✅ **Do:**
1. Separate layers clearly
2. Use Python files for production
3. Define explicit schemas
4. Implement comprehensive quality checks
5. Keep dependencies acyclic
6. Return DataFrames only
7. Use Unity Catalog for secrets
8. Document everything

## Checklist for Production Readiness

- [ ] Explicit schemas defined
- [ ] Data quality rules implemented
- [ ] Medallion layers separated
- [ ] Documentation complete
- [ ] Error handling in place
- [ ] Performance optimized
- [ ] Tests passing
- [ ] Monitoring configured
- [ ] Security reviewed
- [ ] Team trained

---

*Follow these practices to build reliable, maintainable, production-grade data pipelines.*
