# Getting Started with Databricks Lakeflow

Welcome! This guide will help you get up and running with the code examples.

## Prerequisites

Before you begin, ensure you have:

- ✅ Databricks workspace (AWS, Azure, or GCP)
- ✅ Unity Catalog enabled
- ✅ Databricks Runtime 13.3 LTS or higher
- ✅ Appropriate permissions to create pipelines
- ✅ Access to storage locations (S3, ADLS, GCS)

## Quick Start (5 minutes)

### 1. Clone or Download Repository

```bash
git clone https://github.com/yourusername/databricks-lakeflow-examples.git
cd databricks-lakeflow-examples
```

### 2. Upload to Databricks Workspace

**Option A: Using Databricks UI**
1. Open your Databricks workspace
2. Navigate to **Workspace** → **Import**
3. Select **Import from: URL** or **File**
4. Choose the `notebooks/` directory
5. Click **Import**

**Option B: Using Databricks CLI**
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload notebooks
databricks workspace import_dir \
  notebooks/ \
  /Workspace/Users/your-email@company.com/lakeflow-examples/
```

### 3. Create Your First Pipeline

1. In Databricks UI, go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   - **Name**: My First Lakeflow Pipeline
   - **Source Code**: Select `01_introduction_to_lakeflow.py`
   - **Storage Location**: `/mnt/pipelines/my-first-pipeline`
   - **Target Schema**: `main.lakeflow_demo`
4. Click **Create**
5. Click **Start** to run the pipeline

### 4. Monitor Execution

Watch your pipeline run:
- **Lineage Graph**: See data flow visualization
- **Data Quality**: Check expectation metrics
- **Event Log**: Monitor detailed execution

## Learning Path

Follow these notebooks in order:

| # | Notebook | Time | Difficulty |
|---|----------|------|------------|
| 1 | Introduction to Lakeflow | 15 min | 🟢 Beginner |
| 2 | Streaming Tables (Bronze) | 20 min | 🟢 Beginner |
| 3 | Materialized Views (Silver/Gold) | 25 min | 🟢 Beginner |
| 4 | Data Quality Expectations | 20 min | 🟡 Intermediate |
| 5 | Medallion Architecture (Complete) | 30 min | 🟡 Intermediate |
| 6 | CDC and SCD Type 2 | 25 min | 🔴 Advanced |
| 7 | Migration Guide (DLT → Lakeflow) | 20 min | 🟡 Intermediate |
| 8 | Production Best Practices | 30 min | 🔴 Advanced |

**Total Time**: ~3 hours

## Sample Data

For testing, you can use:

1. **Built-in Databricks Datasets**
```python
# In notebook
df = spark.read.table("samples.nyctaxi.trips")
```

2. **Generate Synthetic Data**
```python
from pyspark.sql.functions import *

# Create sample customer data
customers = spark.range(1000) \
    .withColumn("customer_id", col("id").cast("string")) \
    .withColumn("email", concat(lit("user"), col("id"), lit("@example.com"))) \
    .withColumn("country", array(lit("US"), lit("UK"), lit("CA"))[col("id") % 3])

# Write to bronze layer
customers.write.format("delta").save("/mnt/raw/customers")
```

## Troubleshooting

### "Module not found: pyspark.pipelines"
- Ensure Databricks Runtime 13.3 LTS or higher
- This module only works in pipeline contexts

### "Permission denied" errors
- Check Unity Catalog permissions
- Verify storage location access
- Ensure you have CREATE SCHEMA privileges

### Pipeline stuck in "Starting"
- Check cluster availability
- Verify storage path is writable
- Review event logs for errors

For more issues, see [docs/troubleshooting.md](docs/troubleshooting.md)

## Next Steps

1. ✅ Complete the Introduction notebook
2. ✅ Experiment with your own data sources
3. ✅ Join the DataMindAI community
4. ✅ Star this repository
5. ✅ Share your learnings!

## Get Help

- 📚 [Syntax Reference](docs/syntax_reference.md)
- 🐛 [Troubleshooting Guide](docs/troubleshooting.md)
- 💬 [Open an Issue](https://github.com/yourusername/databricks-lakeflow-examples/issues)
- 🌐 [Visit DataMindAI](https://datamindaiwithhmed.com)

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

**Happy Learning!** 🚀

© 2026 DataMindAI | Turn Your Data Into Decision-Ready Intelligence
