"""
# 07 - Migration Guide: DLT → Lakeflow

## Overview
Step-by-step guide to migrate from Delta Live Tables (@dlt) to Lakeflow Pipelines (@dp).

## Migration Strategy
1. Convert imports
2. Update decorators
3. Move from notebooks to .py files
4. Test in development
5. Deploy to production

## Author
Ahmed Mahmoud - DataMindAI
"""

# ==============================================================================
# MIGRATION REFERENCE TABLE
# ==============================================================================

"""
## Syntax Migration Reference

| Old (DLT) | New (Lakeflow @dp) | Notes |
|-----------|-------------------|-------|
| `import dlt` | `from pyspark import pipelines as dp` | New import |
| `@dlt.table(streaming=True)` | `@dp.table` | Streaming table |
| `@dlt.table()` | `@dp.materialized_view` | Batch/materialized |
| `@dlt.expect()` | `@dp.expect()` | Quality check |
| `@dlt.expect_or_drop()` | `@dp.expect_or_drop()` | Same |
| `@dlt.expect_or_fail()` | `@dp.expect_or_fail()` | Same |
| `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` | CDC function |
| `dlt.read()` | `spark.read.table()` | Standard read |
| `dlt.read_stream()` | `spark.readStream.table()` | Streaming read |
"""

# ==============================================================================
# EXAMPLE 1: Basic Table Migration
# ==============================================================================

"""
BEFORE (DLT):
```python
import dlt

@dlt.table(
    name="customers",
    comment="Customer data",
    table_properties={"quality": "silver"}
)
def customers():
    return spark.read.table("raw_customers")
```

AFTER (Lakeflow):
"""

from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Customer data",
    table_properties={"quality": "silver"}
)
def customers():
    return spark.read.table("raw_customers")

"""
Changes:
- Import changed to dp
- @dlt.table() → @dp.materialized_view
- name parameter removed (function name becomes table name)
- Everything else stays the same
"""

# ==============================================================================
# EXAMPLE 2: Streaming Table Migration
# ==============================================================================

"""
BEFORE (DLT):
```python
@dlt.table(
    name="raw_orders",
    streaming=True,
    comment="Raw orders from S3"
)
def raw_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/orders")
```

AFTER (Lakeflow):
"""

@dp.table(
    comment="Raw orders from S3"
)
def raw_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/orders")

"""
Changes:
- @dlt.table(streaming=True) → @dp.table
- streaming parameter removed (implicit in @dp.table)
- name parameter removed
"""

# ==============================================================================
# EXAMPLE 3: Expectations Migration
# ==============================================================================

"""
BEFORE (DLT):
```python
@dlt.table()
@dlt.expect("valid_email", "email IS NOT NULL")
@dlt.expect_or_drop("valid_age", "age >= 18")
def clean_customers():
    return dlt.read("raw_customers").select("*")
```

AFTER (Lakeflow):
"""

@dp.materialized_view()
@dp.expect("valid_email", "email IS NOT NULL")
@dp.expect_or_drop("valid_age", "age >= 18")
def clean_customers():
    return spark.read.table("raw_customers").select("*")

"""
Changes:
- @dlt.table() → @dp.materialized_view()
- @dlt.expect → @dp.expect (same API!)
- dlt.read() → spark.read.table()
"""

# ==============================================================================
# EXAMPLE 4: CDC Migration
# ==============================================================================

"""
BEFORE (DLT):
```python
dlt.apply_changes(
    target="customers_scd2",
    source="raw_customer_changes",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
```

AFTER (Lakeflow):
"""

dp.create_auto_cdc_flow(
    target="customers_scd2",
    source="raw_customer_changes",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)

"""
Changes:
- dlt.apply_changes() → dp.create_auto_cdc_flow()
- All parameters remain the same
- Function name more descriptive
"""

# ==============================================================================
# MIGRATION CHECKLIST
# ==============================================================================

"""
## Step-by-Step Migration

### Step 1: Convert Notebooks to .py Files ✓
1. Create `/transformations` directory
2. Copy notebook cells to .py file
3. Remove magic commands (%sql, %md, etc.)
4. Keep only Python code and docstrings

### Step 2: Update Imports ✓
```python
# Find and replace
# OLD: import dlt
# NEW: from pyspark import pipelines as dp
```

### Step 3: Update Decorators ✓
- @dlt.table(streaming=True) → @dp.table
- @dlt.table() → @dp.materialized_view
- Remove 'name' parameters (use function name)

### Step 4: Update Read Statements ✓
- dlt.read("table") → spark.read.table("table")
- dlt.read_stream("table") → spark.readStream.table("table")

### Step 5: Explicit Schemas ✓
Add schema definitions where inference was used:
```python
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True)
])

@dp.table()
def my_table():
    return spark.readStream.schema(schema).format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/path")
```

### Step 6: Separate Concerns ✓
Modularize into separate files:
- `bronze_layer.py` - Raw ingestion
- `silver_layer.py` - Cleansing
- `gold_layer.py` - Aggregations

### Step 7: Test in Development ✓
1. Create test pipeline in dev workspace
2. Run with small dataset
3. Validate outputs
4. Check data quality metrics

### Step 8: Deploy to Production ✓
1. Update pipeline configuration
2. Point to new .py file(s)
3. Run in full mode first
4. Switch to incremental mode
5. Monitor for issues

## Common Gotchas

### ❌ Using .collect() or .count()
```python
# WRONG
@dp.materialized_view()
def bad_table():
    df = spark.read.table("source")
    count = df.count()  # ❌ Don't do this
    return df
```

### ✓ Correct Approach
```python
# RIGHT
@dp.materialized_view()
def good_table():
    return spark.read.table("source")  # ✓ Just return DataFrame
```

### ❌ Mixing Streaming and Batch
```python
# WRONG
@dp.table()  # Streaming decorator
def mixed_table():
    return spark.read.table("source")  # ❌ Batch read
```

### ✓ Match Read Type to Decorator
```python
# RIGHT
@dp.table()  # Streaming decorator
def streaming_table():
    return spark.readStream.table("source")  # ✓ Streaming read

@dp.materialized_view()  # Batch decorator
def batch_table():
    return spark.read.table("source")  # ✓ Batch read
```

## Testing Your Migration

```python
# Create test pipeline
from databricks import pipelines

test_pipeline = pipelines.create(
    name="migration_test",
    storage="/dbfs/pipelines/test/",
    target="test_schema",
    libraries=[{"file": {"path": "/path/to/migrated.py"}}],
    development=True  # Run in dev mode first
)

# Start and monitor
pipelines.start(test_pipeline.pipeline_id)
```

## Rollback Plan

If issues arise:
1. Keep old DLT pipeline available
2. Can switch back by changing source path
3. Delta tables are forward/backward compatible
4. Test thoroughly before decommissioning old pipeline

## Next Steps

See `08_best_practices_production.py` for production-ready
patterns and optimization strategies.
"""
