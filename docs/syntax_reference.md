# Lakeflow Syntax Reference

Quick reference guide for migrating from DLT to Lakeflow (@dp decorators).

## Import Statements

```python
# OLD (DLT)
import dlt

# NEW (Lakeflow)
from pyspark import pipelines as dp
```

## Table Decorators

### Streaming Tables

```python
# OLD (DLT)
@dlt.table(
    name="my_table",
    streaming=True,
    comment="My streaming table"
)

# NEW (Lakeflow)
@dp.table(
    comment="My streaming table"
)
# Note: name removed (uses function name), streaming implicit
```

### Batch/Materialized Views

```python
# OLD (DLT)
@dlt.table(
    name="my_view",
    comment="My batch view"
)

# NEW (Lakeflow)
@dp.materialized_view(
    comment="My batch view"
)
```

## Data Quality Expectations

```python
# All three are identical between DLT and Lakeflow
@dp.expect("rule_name", "condition")
@dp.expect_or_drop("rule_name", "condition")
@dp.expect_or_fail("rule_name", "condition")
```

## Reading Data

### From Tables

```python
# OLD (DLT)
dlt.read("table_name")
dlt.read_stream("table_name")

# NEW (Lakeflow)
spark.read.table("table_name")
spark.readStream.table("table_name")
```

### From Cloud Storage

```python
# Same in both DLT and Lakeflow
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("/path/to/data")
```

## CDC Flows

```python
# OLD (DLT)
dlt.apply_changes(
    target="target_table",
    source="source_table",
    keys=["id"],
    sequence_by="timestamp"
)

# NEW (Lakeflow)
dp.create_auto_cdc_flow(
    target="target_table",
    source="source_table",
    keys=["id"],
    sequence_by="timestamp"
)
```

## Complete Example Comparison

### OLD (DLT)
```python
import dlt

@dlt.table(
    name="bronze_customers",
    streaming=True,
    comment="Raw customer data"
)
def bronze_customers():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/customers")

@dlt.table(
    name="silver_customers",
    comment="Cleaned customers"
)
@dlt.expect("valid_email", "email IS NOT NULL")
@dlt.expect_or_drop("valid_age", "age >= 18")
def silver_customers():
    return dlt.read("bronze_customers") \
        .select("customer_id", "email", "age")
```

### NEW (Lakeflow)
```python
from pyspark import pipelines as dp

@dp.table(
    comment="Raw customer data"
)
def bronze_customers():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/customers")

@dp.materialized_view(
    comment="Cleaned customers"
)
@dp.expect("valid_email", "email IS NOT NULL")
@dp.expect_or_drop("valid_age", "age >= 18")
def silver_customers():
    return spark.read.table("bronze_customers") \
        .select("customer_id", "email", "age")
```

## Key Differences Summary

| Aspect | DLT | Lakeflow |
|--------|-----|----------|
| Import | `import dlt` | `from pyspark import pipelines as dp` |
| Streaming | `@dlt.table(streaming=True)` | `@dp.table` |
| Batch | `@dlt.table()` | `@dp.materialized_view` |
| Table name | `name="..."` parameter | Function name |
| Read table | `dlt.read("...")` | `spark.read.table("...")` |
| CDC | `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |

## Common Mistakes

### ❌ Mixing Decorators and Reads
```python
# WRONG
@dp.table()  # Streaming decorator
def my_table():
    return spark.read.table("source")  # Batch read - mismatch!
```

### ✅ Correct Pattern
```python
# RIGHT - Streaming decorator with streaming read
@dp.table()
def my_table():
    return spark.readStream.table("source")

# RIGHT - Batch decorator with batch read
@dp.materialized_view()
def my_view():
    return spark.read.table("source")
```

### ❌ Using Actions in Definitions
```python
# WRONG
@dp.materialized_view()
def bad_view():
    df = spark.read.table("source")
    count = df.count()  # ❌ No actions allowed
    return df.filter(col("id").isNotNull())
```

### ✅ Correct Pattern
```python
# RIGHT
@dp.materialized_view()
def good_view():
    return spark.read.table("source") \
        .filter(col("id").isNotNull())
```
