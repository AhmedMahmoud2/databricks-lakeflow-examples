# Troubleshooting Guide

Common issues and solutions when working with Databricks Lakeflow.

## Issue: "pipelines module not found"

**Error:**
```
ModuleNotFoundError: No module named 'pyspark.pipelines'
```

**Solution:**
- Ensure you're using Databricks Runtime 13.3 LTS or higher
- The `pipelines` module is only available in pipeline contexts
- Cannot test locally without Databricks environment

## Issue: "Cannot use actions in pipeline definitions"

**Error:**
```
AnalysisException: Cannot perform action (count, collect, etc.) inside pipeline definition
```

**Solution:**
```python
# ❌ WRONG
@dp.materialized_view()
def bad_table():
    df = spark.read.table("source")
    count = df.count()  # Don't do this
    return df

# ✅ CORRECT
@dp.materialized_view()
def good_table():
    return spark.read.table("source")  # Just return the DataFrame
```

## Issue: "Streaming source with batch decorator"

**Error:**
```
StreamingQueryException: Cannot start streaming query with batch decorator
```

**Solution:**
Match decorator type to read type:
```python
# ✅ Streaming read → @dp.table
@dp.table()
def streaming_table():
    return spark.readStream.table("source")

# ✅ Batch read → @dp.materialized_view
@dp.materialized_view()
def batch_table():
    return spark.read.table("source")
```

## Issue: "Checkpoint location conflicts"

**Error:**
```
ConcurrentModificationException: Checkpoint directory being used by another stream
```

**Solution:**
- Each streaming source needs unique checkpoint location
- Auto Loader manages checkpoints automatically
- Don't manually specify checkpoint locations unless necessary
- Delete old checkpoints when restarting from scratch

## Issue: "Schema inference too expensive"

**Problem:**
Pipeline runs slowly and costs are high due to schema inference.

**Solution:**
Always use explicit schemas in production:
```python
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True)
])

@dp.table()
def optimized_table():
    return spark.readStream \
        .schema(schema) \  # Explicit schema
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/path")
```

## Issue: "Expectation failures not showing"

**Problem:**
Data quality issues not visible in UI.

**Solution:**
- Check the Data Quality tab in pipeline UI
- Expectations only logged when decorator is used
- Use monitoring queries to track failures:
```python
@dp.materialized_view()
def quality_monitoring():
    return spark.read.table("event_log") \
        .filter(col("event_type") == "data_quality")
```

## Issue: "Pipeline stuck in 'Starting' state"

**Possible Causes:**
1. Cluster startup issues
2. Resource quota exceeded
3. Storage location permissions

**Solutions:**
- Check cluster logs for errors
- Verify storage path is writable
- Ensure sufficient quota
- Try serverless compute

## Issue: "Out of memory errors"

**Error:**
```
OutOfMemoryError: Java heap space
```

**Solutions:**
1. Reduce batch size:
```python
.option("cloudFiles.maxFilesPerTrigger", "100")  # Reduce from default
```

2. Increase cluster size or use serverless

3. Optimize transformations:
```python
# ❌ Don't do this
df.groupBy("col1", "col2", "col3", "col4").agg(...)  # Too many groups

# ✅ Do this
df.groupBy("col1", "col2").agg(...)  # Fewer groups
```

## Issue: "Table not found"

**Error:**
```
AnalysisException: Table or view not found: my_table
```

**Solutions:**
- Ensure upstream table ran successfully
- Check table name matches function name
- Verify schema/catalog is correct
- Check Unity Catalog permissions

## Getting Help

1. Check Databricks documentation: https://docs.databricks.com
2. Review pipeline logs in UI
3. Contact DataMindAI: https://datamindaiwithhmed.com
4. Open GitHub issue: https://github.com/yourusername/repo
