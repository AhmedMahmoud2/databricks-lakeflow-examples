"""
# 04 - Data Quality as Code

## Overview
Implement robust data quality rules using @dp.expect decorators.
Integrate quality checks directly into your pipeline definitions.

## What You'll Learn
- Three levels of expectations: warn, drop, fail
- Managing expectations in Unity Catalog
- Custom validation rules
- Monitoring and alerting on quality issues

## Author
Ahmed Mahmoud - DataMindAI
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ==============================================================================
# SECTION 1: The Three Levels of Expectations
# ==============================================================================

"""
## Data Quality Levels

1. @dp.expect - WARN: Log violations, keep data
2. @dp.expect_or_drop - CLEAN: Drop invalid rows, continue
3. @dp.expect_or_fail - HALT: Stop pipeline on violation

Choose based on criticality of the rule.
"""

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

"""
## Advanced Validation Logic

Use SQL expressions for complex rules.
"""

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
""")
def silver_transactions_validated():
    """
    Financial data requires strict validation:
    - All amounts must be positive and reasonable
    - Only approved currencies
    - Valid status codes
    - No future-dated transactions
    - All critical fields required
    """
    return spark.read.table("bronze_transactions") \
        .select(
            "transaction_id",
            "customer_id",
            "amount",
            "currency",
            "status",
            "transaction_date"
        )

# ==============================================================================
# SECTION 3: Business Rule Validations
# ==============================================================================

"""
## Enforcing Business Logic

Validate data against business rules.
"""

@dp.materialized_view(
    comment="Orders with business rule validations"
)
@dp.expect("reasonable_quantity", "quantity >= 1 AND quantity <= 1000")
@dp.expect("positive_discount", "discount >= 0 AND discount <= 100")
@dp.expect("valid_total", "total_amount = (unit_price * quantity) - (unit_price * quantity * discount / 100)")
@dp.expect_or_drop("delivery_date_after_order", "delivery_date >= order_date")
def silver_orders_validated():
    """
    Business rules:
    - Quantity must be reasonable (1-1000)
    - Discount percentage valid (0-100%)
    - Total matches calculation
    - Delivery can't be before order
    """
    return spark.read.table("bronze_orders") \
        .withColumn(
            "calculated_total",
            (col("unit_price") * col("quantity")) - 
            (col("unit_price") * col("quantity") * col("discount") / 100)
        ) \
        .select(
            "order_id",
            "customer_id",
            "quantity",
            "unit_price",
            "discount",
            "calculated_total",
            "order_date",
            "delivery_date"
        )

# ==============================================================================
# SECTION 4: Reference Data Validations
# ==============================================================================

"""
## Validating Against Reference Data

Check values against lookup tables.
"""

@dp.materialized_view(
    comment="Sales with country code validation"
)
@dp.expect("valid_country_code", "country_code IN (SELECT code FROM valid_countries)")
@dp.expect("valid_product_id", "product_id IN (SELECT product_id FROM valid_products)")
def silver_sales_with_lookups():
    """
    Validate foreign keys against reference tables:
    - Country codes must exist in valid_countries
    - Product IDs must exist in valid_products
    """
    return spark.read.table("bronze_sales") \
        .select(
            "sale_id",
            "customer_id",
            "product_id",
            "country_code",
            "amount",
            "sale_date"
        )

# ==============================================================================
# SECTION 5: Time-Based Validations
# ==============================================================================

"""
## Temporal Data Quality

Ensure dates and timestamps are valid.
"""

@dp.materialized_view(
    comment="Events with temporal validations"
)
@dp.expect("not_future_dated", "event_timestamp <= CURRENT_TIMESTAMP()")
@dp.expect("recent_data", "event_timestamp >= DATE_SUB(CURRENT_DATE(), 365)")
@dp.expect_or_drop("valid_date_range", "event_timestamp >= '2020-01-01'")
def silver_events_temporal():
    """
    Temporal validations:
    - Events can't be in the future
    - Data should be recent (within 1 year)
    - Must be after system inception date
    """
    return spark.read.table("bronze_events") \
        .select(
            "event_id",
            "event_type",
            "event_timestamp",
            "user_id"
        )

# ==============================================================================
# KEY CONCEPTS SUMMARY
# ==============================================================================

"""
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
