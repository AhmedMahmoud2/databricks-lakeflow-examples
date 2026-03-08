# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 04 - Data Quality as Code
# MAGIC
# MAGIC ## Overview
# MAGIC Implement robust data quality rules using @dp.expect decorators.
# MAGIC Integrate quality checks directly into your pipeline definitions.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - Three levels of expectations: warn, drop, fail
# MAGIC - Managing expectations in Unity Catalog
# MAGIC - Custom validation rules
# MAGIC - Monitoring and alerting on quality issues
# MAGIC
# MAGIC ## Author
# MAGIC Ahmed Mahmoud - DataMindAI
# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ==============================================================================
# SECTION 1: The Three Levels of Expectations
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Levels
# MAGIC
# MAGIC 1. @dp.expect - WARN: Log violations, keep data
# MAGIC 2. @dp.expect_or_drop - CLEAN: Drop invalid rows, continue
# MAGIC 3. @dp.expect_or_fail - HALT: Stop pipeline on violation
# MAGIC
# MAGIC Choose based on criticality of the rule.
# COMMAND ----------
@dp.materialized_view(
    comment="Customer data with multi-level quality checks"
)
@dp.expect("valid_email_format", "email LIKE '%@%'")
@dp.expect_or_drop("valid_age", "age >= 18 AND age <= 120")
@dp.expect_or_fail("required_fields", "customer_id IS NOT NULL AND email IS NOT NULL")
def silver_customers_validated():
    """
    Three-tier data quality strategy:
    
    Level 1 (WARN): Email format - log but keep
    Level 2 (CLEAN): Age validation - drop invalid
    Level 3 (HALT): Required fields - fail pipeline
    """
    return spark.read.table("bronze_customers") \
        .select(
            "customer_id",
            "first_name", 
            "last_name",
            "email",
            "age",
            "country",
            "created_at"
        )

# ==============================================================================
# SECTION 2: Complex Validation Rules
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Advanced Validation Logic
# MAGIC
# MAGIC Use SQL expressions for complex rules.
# COMMAND ----------
@dp.materialized_view(
    comment="Financial transactions with strict validations"
)
@dp.expect("valid_amount", "amount > 0 AND amount < 1000000")
@dp.expect("valid_currency", "currency IN ('USD', 'EUR', 'GBP')")
@dp.expect("valid_status", "status IN ('pending', 'completed', 'cancelled')")
@dp.expect_or_drop("future_date_check", "transaction_date <= CURRENT_DATE()")
@dp.expect_or_fail("critical_fields", """
    transaction_id IS NOT NULL AND 
    customer_id IS NOT NULL AND 
    amount IS NOT NULL AND
    currency IS NOT NULL
# COMMAND ----------
# MAGIC %md
# MAGIC )
# MAGIC def silver_transactions_validated():
# MAGIC     """
# MAGIC     Financial data requires strict validation:
# MAGIC     - All amounts must be positive and reasonable
# MAGIC     - Only approved currencies
# MAGIC     - Valid status codes
# MAGIC     - No future-dated transactions
# MAGIC     - All critical fields required
# MAGIC     """
# MAGIC     return spark.read.table("bronze_transactions") \
# MAGIC         .select(
# MAGIC             "transaction_id",
# MAGIC             "customer_id",
# MAGIC             "amount",
# MAGIC             "currency",
# MAGIC             "status",
# MAGIC             "transaction_date"
# MAGIC         )
# MAGIC
# MAGIC # ==============================================================================
# MAGIC # SECTION 3: Business Rule Validations
# MAGIC # ==============================================================================
# COMMAND ----------
## Enforcing Business Logic

Validate data against business rules.
# COMMAND ----------
# MAGIC %md
# MAGIC @dp.materialized_view(
# MAGIC     comment="Orders with business rule validations"
# MAGIC )
# MAGIC @dp.expect("reasonable_quantity", "quantity >= 1 AND quantity <= 1000")
# MAGIC @dp.expect("positive_discount", "discount >= 0 AND discount <= 100")
# MAGIC @dp.expect("valid_total", "total_amount = (unit_price * quantity) - (unit_price * quantity * discount / 100)")
# MAGIC @dp.expect_or_drop("delivery_date_after_order", "delivery_date >= order_date")
# MAGIC def silver_orders_validated():
# MAGIC     """
# MAGIC     Business rules:
# MAGIC     - Quantity must be reasonable (1-1000)
# MAGIC     - Discount percentage valid (0-100%)
# MAGIC     - Total matches calculation
# MAGIC     - Delivery can't be before order
# MAGIC     """
# MAGIC     return spark.read.table("bronze_orders") \
# MAGIC         .withColumn(
# MAGIC             "calculated_total",
# MAGIC             (col("unit_price") * col("quantity")) - 
# MAGIC             (col("unit_price") * col("quantity") * col("discount") / 100)
# MAGIC         ) \
# MAGIC         .select(
# MAGIC             "order_id",
# MAGIC             "customer_id",
# MAGIC             "quantity",
# MAGIC             "unit_price",
# MAGIC             "discount",
# MAGIC             "calculated_total",
# MAGIC             "order_date",
# MAGIC             "delivery_date"
# MAGIC         )
# MAGIC
# MAGIC # ==============================================================================
# MAGIC # SECTION 4: Reference Data Validations
# MAGIC # ==============================================================================
# COMMAND ----------
## Validating Against Reference Data

Check values against lookup tables.
# COMMAND ----------
# MAGIC %md
# MAGIC @dp.materialized_view(
# MAGIC     comment="Sales with country code validation"
# MAGIC )
# MAGIC @dp.expect("valid_country_code", "country_code IN (SELECT code FROM valid_countries)")
# MAGIC @dp.expect("valid_product_id", "product_id IN (SELECT product_id FROM valid_products)")
# MAGIC def silver_sales_with_lookups():
# MAGIC     """
# MAGIC     Validate foreign keys against reference tables:
# MAGIC     - Country codes must exist in valid_countries
# MAGIC     - Product IDs must exist in valid_products
# MAGIC     """
# MAGIC     return spark.read.table("bronze_sales") \
# MAGIC         .select(
# MAGIC             "sale_id",
# MAGIC             "customer_id",
# MAGIC             "product_id",
# MAGIC             "country_code",
# MAGIC             "amount",
# MAGIC             "sale_date"
# MAGIC         )
# MAGIC
# MAGIC # ==============================================================================
# MAGIC # SECTION 5: Time-Based Validations
# MAGIC # ==============================================================================
# COMMAND ----------
## Temporal Data Quality

Ensure dates and timestamps are valid.
# COMMAND ----------
# MAGIC %md
# MAGIC @dp.materialized_view(
# MAGIC     comment="Events with temporal validations"
# MAGIC )
# MAGIC @dp.expect("not_future_dated", "event_timestamp <= CURRENT_TIMESTAMP()")
# MAGIC @dp.expect("recent_data", "event_timestamp >= DATE_SUB(CURRENT_DATE(), 365)")
# MAGIC @dp.expect_or_drop("valid_date_range", "event_timestamp >= '2020-01-01'")
# MAGIC def silver_events_temporal():
# MAGIC     """
# MAGIC     Temporal validations:
# MAGIC     - Events can't be in the future
# MAGIC     - Data should be recent (within 1 year)
# MAGIC     - Must be after system inception date
# MAGIC     """
# MAGIC     return spark.read.table("bronze_events") \
# MAGIC         .select(
# MAGIC             "event_id",
# MAGIC             "event_type",
# MAGIC             "event_timestamp",
# MAGIC             "user_id"
# MAGIC         )
# MAGIC
# MAGIC # ==============================================================================
# MAGIC # KEY CONCEPTS SUMMARY
# MAGIC # ==============================================================================
# COMMAND ----------
## Data Quality Best Practices

### ✅ DO:
1. Layer expectations by criticality (warn → drop → fail)
2. Use descriptive expectation names
3. Test expectations with sample data first
4. Monitor expectation failure rates
5. Document business rules in comments
6. Use Unity Catalog to manage shared rules (2026)

### ❌ DON'T:
1. Over-use @dp.expect_or_fail (makes pipeline fragile)
2. Write vague expectation names like "check1"
3. Duplicate validation logic across tables
4. Ignore expectation metrics in monitoring
5. Skip testing edge cases

### 💡 Pro Tips:
- Start with @dp.expect, promote to _or_drop if needed
- Use @dp.expect_or_fail only for truly critical rules
- Create reusable validation functions
- Track expectation violations over time
- Alert on sudden spikes in violations

## 2026 Feature: Unity Catalog Expectations

Store and manage expectations centrally:

```python
# Store expectations in Unity Catalog
dp.create_expectation(
    catalog="main",
    schema="quality_rules",
    name="valid_email",
    constraint="email LIKE '%@%' AND email NOT LIKE '%test%'"
)

# Reference in pipelines
@dp.expect_from_catalog("main.quality_rules.valid_email")
def my_table():
    return spark.read.table("bronze_table")
```

Benefits:
- Version-controlled quality rules
- Reusable across pipelines
- Centralized governance
- Audit trail of rule changes

## Next Steps

Proceed to `05_medallion_architecture_complete.py` to see
how all layers work together in a complete pipeline.
"""
