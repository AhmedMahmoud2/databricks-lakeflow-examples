"""
# 06 - CDC and SCD Type 2

## Overview
Implement Change Data Capture with Slowly Changing Dimensions Type 2.
Track complete history of changes with automatic timeline management.

## What You'll Learn
- Using dp.create_auto_cdc_flow()
- SCD Type 2 pattern implementation
- Multiple views from single CDC source
- History tracking with START_AT and END_AT

## Author
Ahmed Mahmoud - DataMindAI
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ==============================================================================
# SECTION 1: Basic CDC Flow
# ==============================================================================

"""
## The create_auto_cdc_flow Function

Automatically handles:
- Inserts: New records
- Updates: Modified records (with history)
- Deletes: Soft deletes with end timestamp
"""

# Source: Raw change data
@dp.table(comment="CDC source from database")
def raw_customer_changes():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/cdc/customers/")

# Target: SCD Type 2 table
dp.create_auto_cdc_flow(
    source="raw_customer_changes",
    keys=["customer_id"],
    sequence_by="updated_at",
    target="silver_customers_scd2",
    stored_as_scd_type=2
)

"""
What this does:
- customer_id: Primary key for tracking records
- updated_at: Timestamp to order changes
- Creates SCD Type 2 with __START_AT and __END_AT columns
- __END_AT = NULL for current version
- Previous versions have __END_AT set
"""

# ==============================================================================
# SECTION 2: Multiple Views from Single CDC
# ==============================================================================

"""
## 1-to-Many Pattern

Create multiple regional views from one CDC source.
This is a 2026 efficiency upgrade!
"""

# Single CDC source
@dp.table(comment="Global customer changes")
def cdc_customers_global():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/cdc/customers_global/")

# Region A view
dp.create_auto_cdc_flow(
    source="cdc_customers_global",
    keys=["customer_id"],
    sequence_by="updated_at",
    target="customers_region_a_scd2",
    where="region = 'A'",
    stored_as_scd_type=2
)

# Region B view
dp.create_auto_cdc_flow(
    source="cdc_customers_global",
    keys=["customer_id"],
    sequence_by="updated_at",
    target="customers_region_b_scd2",
    where="region = 'B'",
    stored_as_scd_type=2
)

"""
Benefits:
- Process CDC stream once
- Fan out to multiple views
- Each view maintains own history
- Significant cost savings
"""

# ==============================================================================
# SECTION 3: Querying SCD Type 2 Tables
# ==============================================================================

"""
## Working with Historical Data

Query patterns for SCD Type 2 tables.
"""

@dp.materialized_view(comment="Current customer snapshot")
def customers_current():
    """
    Get only current versions of all customers.
    """
    return spark.read.table("silver_customers_scd2") \
        .filter(col("__END_AT").isNull()) \
        .select("customer_id", "first_name", "last_name", "email", "country")

@dp.materialized_view(comment="Customer history with effective dates")
def customers_history():
    """
    Full history with effective date ranges.
    """
    return spark.read.table("silver_customers_scd2") \
        .select(
            "customer_id",
            "first_name",
            "last_name", 
            "email",
            "country",
            col("__START_AT").alias("effective_from"),
            col("__END_AT").alias("effective_to"),
            when(col("__END_AT").isNull(), lit(True)).otherwise(lit(False)).alias("is_current")
        )

@dp.materialized_view(comment="Point-in-time customer view")
def customers_as_of_date():
    """
    Get customer state as of specific date.
    Example: What did data look like on 2024-01-01?
    """
    point_in_time = "2024-01-01"
    
    return spark.read.table("silver_customers_scd2") \
        .filter(
            (col("__START_AT") <= point_in_time) &
            ((col("__END_AT") > point_in_time) | col("__END_AT").isNull())
        ) \
        .select("customer_id", "first_name", "last_name", "email", "country")

# ==============================================================================
# SECTION 4: CDC with Complex Keys
# ==============================================================================

"""
## Composite Keys and Multiple Sequences

Handle complex CDC scenarios.
"""

@dp.table(comment="Order line items CDC")
def raw_order_items_cdc():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/cdc/order_items/")

# Composite key: order_id + line_number
dp.create_auto_cdc_flow(
    source="raw_order_items_cdc",
    keys=["order_id", "line_number"],  # Composite key
    sequence_by="updated_at",
    target="silver_order_items_scd2",
    stored_as_scd_type=2
)

"""
## Key Concepts

### Automatic Timeline Management
- __START_AT: When this version became effective
- __END_AT: When this version was superseded (NULL = current)
- __DELETED: Boolean flag for soft deletes

### Change Types Handled
1. INSERT: New record appears
2. UPDATE: Existing record modified (closes old, opens new)
3. DELETE: Record soft-deleted (__DELETED = true, __END_AT set)

### Best Practices
- Always use timestamp for sequence_by
- Use composite keys when needed
- Filter WHERE __END_AT IS NULL for current state
- Use BETWEEN for point-in-time queries
- Consider data retention policies for history

## Next Steps

See `07_migration_dlt_to_dp.py` for migrating existing
Delta Live Tables pipelines to new @dp syntax.
"""
