# Mastering Databricks Lakeflow - Code Examples

[![Databricks](https://img.shields.io/badge/Databricks-Lakeflow-FF3621?style=flat&logo=databricks)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Companion code repository for the **"Mastering Databricks Lakeflow"** presentation by [DataMindAI with Ahmed](https://datamindaiwithhmed.com).

## 📚 Overview

This repository contains practical examples demonstrating the evolution from Delta Live Tables (DLT) to Databricks Lakeflow Pipelines using the new `@dp` decorator syntax (2026 Standard).

### What is Lakeflow?

Databricks Lakeflow is a unified solution for data engineering that encompasses:
- **Lakeflow Connect**: Native ingestion from databases and apps using CDC
- **Lakeflow Pipelines**: Declarative transformation pipelines (evolution of DLT)
- **Lakeflow Jobs**: Reliable orchestration with full lineage tracking

## 🎯 Learning Objectives

By working through these notebooks, you'll learn how to:

1. ✅ Migrate from imperative notebooks to declarative `.py` files
2. ✅ Use the new `@dp` decorator syntax (replacing `@dlt`)
3. ✅ Build streaming tables with Auto Loader
4. ✅ Create materialized views for aggregations
5. ✅ Implement data quality rules with expectations
6. ✅ Design medallion architecture (Bronze → Silver → Gold)
7. ✅ Set up CDC flows for slowly changing dimensions
8. ✅ Leverage Unity Catalog for governance

## 📂 Repository Structure

```
databricks-lakeflow-examples/
├── README.md                          
├── notebooks/
│   ├── 01_introduction_to_lakeflow.py
│   ├── 02_streaming_tables_bronze.py
│   ├── 03_materialized_views_silver.py
│   ├── 04_data_quality_expectations.py
│   ├── 05_medallion_architecture_complete.py
│   ├── 06_cdc_scd_type2.py
│   ├── 07_migration_dlt_to_dp.py
│   └── 08_best_practices_production.py
├── assets/
│   └── sample_data/                  
├── docs/
│   ├── syntax_reference.md           
│   └── troubleshooting.md            
└── requirements.txt                  
```

## 🚀 Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks Runtime 13.3 LTS or higher
- Python 3.10+

### Setup

1. **Clone this repository**
   ```bash
   git clone https://github.com/yourusername/databricks-lakeflow-examples.git
   cd databricks-lakeflow-examples
   ```

2. **Upload notebooks to Databricks**
   - Use Databricks CLI or workspace UI
   - Navigate to Workflows → Delta Live Tables
   - Create pipeline pointing to these .py files

## 📖 Notebook Guide

All examples use the 2026 standard with `@dp` decorators and `.py` file format.

See individual notebooks for detailed explanations and runnable code.

## 🔑 Key Syntax Changes (2026 Standard)

| Old (DLT) | New (Lakeflow) |
|-----------|----------------|
| `import dlt` | `from pyspark import pipelines as dp` |
| `@dlt.table(streaming=True)` | `@dp.table` |
| `@dlt.table()` | `@dp.materialized_view` |
| `@dlt.expect()` | `@dp.expect()` |
| `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |

## 🎥 Video Tutorial

Watch the complete presentation on YouTube: [Mastering Databricks Lakeflow](https://youtube.com/@datamindaiwithhmed)

## 👨‍💻 Author

**Ahmed Mahmoud**  
Principal Data Engineer | Head of Data & AI Engineering  
DataMindAI

- Website: [datamindaiwithhmed.com](https://datamindaiwithhmed.com)
- LinkedIn: [Connect with Ahmed](https://linkedin.com/in/your-profile)
- YouTube: [@datamindaiwithhmed](https://youtube.com/@datamindaiwithhmed)

---

**© 2026 DataMindAI | Empowering Data Intelligence**
