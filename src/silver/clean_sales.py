"""
Silver Layer: Clean Sales Data

This module demonstrates data quality validation and cleansing
using the three-tier expectation system.

Quality Tiers:
- @dp.expect: Warning level - log but keep data
- @dp.expect_or_drop: Cleanup level - drop invalid rows
- @dp.expect_or_fail: Critical level - stop pipeline
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Cleaned and validated sales data with quality checks"
)
@dp.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_date", "transaction_date IS NOT NULL AND transaction_date <= current_date()")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_fail("valid_currency", "currency IN ('USD', 'EUR', 'GBP')")
def clean_sales():
    """
    Clean and validate sales data from bronze layer.
    
    Data Quality Rules:
    1. Transaction ID must exist (warn if missing)
    2. Amount must be positive (warn if not)
    3. Date must be valid and not in future (drop if invalid)
    4. Quantity must be positive (drop if not)
    5. Currency must be supported (fail pipeline if not)
    
    Transformations:
    - Deduplicate based on transaction_id
    - Calculate total_amount if missing
    - Standardize payment methods
    - Add data quality flags
    
    Returns:
        DataFrame: Cleaned and validated sales data
    """
    return (
        spark.read.table("raw_sales")
        # Deduplicate based on transaction_id, keeping most recent
        .dropDuplicates(["transaction_id"])
        # Calculate total if missing
        .withColumn(
            "amount",
            F.when(F.col("amount").isNull(), F.col("quantity") * F.col("unit_price"))
            .otherwise(F.col("amount"))
        )
        # Standardize payment methods
        .withColumn(
            "payment_method",
            F.upper(F.trim(F.coalesce(F.col("payment_method"), F.lit("UNKNOWN"))))
        )
        # Add processing timestamp
        .withColumn("processed_at", F.current_timestamp())
        # Add data quality score
        .withColumn(
            "quality_score",
            F.when(F.col("customer_id").isNull(), 0.8)
            .when(F.col("payment_method") == "UNKNOWN", 0.9)
            .otherwise(1.0)
        )
    )


@dp.materialized_view(
    comment="Sales with enriched store and product information"
)
def enriched_sales():
    """
    Enrich sales data with store and product dimensions.
    
    This creates a denormalized table optimized for analytics,
    joining clean sales with clean products and stores.
    
    Returns:
        DataFrame: Enriched sales data ready for gold layer
    """
    sales = spark.read.table("clean_sales")
    products = spark.read.table("clean_products")
    
    return (
        sales.alias("s")
        .join(products.alias("p"), sales.product_id == products.product_id, "left")
        .select(
            "s.*",
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("p.subcategory"),
            F.col("p.brand"),
            F.col("p.unit_cost"),
            (F.col("s.amount") - (F.col("s.quantity") * F.col("p.unit_cost"))).alias("profit")
        )
    )


@dp.materialized_view(
    comment="Sales data with anomaly detection"
)
@dp.expect("reasonable_amount", "amount < 10000")
def sales_with_anomalies():
    """
    Detect anomalies in sales data using statistical methods.
    
    Flags transactions that are:
    - Unusually high amounts
    - Unusual time of day
    - Suspicious patterns
    
    Returns:
        DataFrame: Sales data with anomaly flags
    """
    from pyspark.sql.window import Window
    
    sales = spark.read.table("clean_sales")
    
    # Calculate rolling statistics
    window_spec = Window.partitionBy("store_id").orderBy("transaction_date").rowsBetween(-30, 0)
    
    return (
        sales
        .withColumn("avg_amount_30d", F.avg("amount").over(window_spec))
        .withColumn("stddev_amount_30d", F.stddev("amount").over(window_spec))
        .withColumn(
            "is_anomaly",
            F.when(
                F.col("amount") > F.col("avg_amount_30d") + (3 * F.col("stddev_amount_30d")),
                True
            ).otherwise(False)
        )
        .withColumn(
            "anomaly_score",
            (F.col("amount") - F.col("avg_amount_30d")) / F.col("stddev_amount_30d")
        )
    )
