# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 03 - Materialized Views (Silver/Gold Layers)
# MAGIC
# MAGIC ## Overview
# MAGIC Learn to create aggregated, business-ready datasets using @dp.materialized_view.
# MAGIC This is your Silver and Gold layer pattern - cleaned, validated, and business-ready data.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - When to use materialized views vs streaming tables
# MAGIC - Engine-managed refresh strategies
# MAGIC - Complex transformations and aggregations
# MAGIC - Join patterns and window functions
# MAGIC - Performance optimization
# MAGIC
# MAGIC ## Author
# MAGIC Ahmed Mahmoud - DataMindAI
# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ==============================================================================
# SECTION 1: Basic Materialized View
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## The @dp.materialized_view Decorator
# MAGIC
# MAGIC A materialized view is:
# MAGIC - For aggregations, joins, and derived logic
# MAGIC - Engine decides full refresh vs incremental update
# MAGIC - Standard batch read syntax
# MAGIC - Ideal for Silver and Gold layers
# COMMAND ----------
@dp.materialized_view(
    comment="Cleaned customer data (Silver layer)"
)
def silver_customers():
    """
    Basic transformation from Bronze to Silver.
    
    Key operations:
    - Read from bronze layer
    - Filter out invalid records
    - Deduplicate
    - Standardize formats
    - Add derived columns
    """
    return spark.read.table("bronze_customers") \
        .filter(col("email").isNotNull()) \
        .filter(col("email").rlike(".+@.+\\..+")) \
        .dropDuplicates(["customer_id"]) \
        .select(
            "customer_id",
            "first_name",
            "last_name",
            lower(col("email")).alias("email"),
            "phone",
            upper(col("country")).alias("country"),
            "created_at"
        )

# ==============================================================================
# SECTION 2: Aggregations and Group By
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregating Data for Gold Layer
# MAGIC
# MAGIC Create business metrics and KPIs.
# COMMAND ----------
@dp.materialized_view(
    comment="Daily sales summary (Gold layer)"
)
def gold_daily_sales():
    """
    Aggregate sales data by day.
    
    Metrics:
    - Total transactions
    - Total revenue
    - Average order value
    - Unique customers
    """
    return spark.read.table("silver_sales") \
        .groupBy(
            to_date(col("sale_date")).alias("date")
        ) \
        .agg(
            count("sale_id").alias("total_transactions"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("date")

# ==============================================================================
# SECTION 3: Joins Across Tables
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Joining Multiple Tables
# MAGIC
# MAGIC Create enriched datasets by combining multiple sources.
# COMMAND ----------
@dp.materialized_view(
    comment="Enriched sales with customer and product details"
)
def silver_sales_enriched():
    """
    Join sales with customer and product dimensions.
    
    Join strategy:
    - Inner join ensures referential integrity
    - Select relevant columns from each table
    - Rename ambiguous columns
    """
    sales = spark.read.table("bronze_sales")
    customers = spark.read.table("silver_customers")
    products = spark.read.table("silver_products")
    
    return sales \
        .join(customers, "customer_id", "inner") \
        .join(products, "product_id", "inner") \
        .select(
            sales["sale_id"],
            sales["sale_date"],
            sales["amount"],
            customers["customer_id"],
            concat(customers["first_name"], lit(" "), customers["last_name"]).alias("customer_name"),
            customers["country"].alias("customer_country"),
            products["product_id"],
            products["product_name"],
            products["category"]
        )

# ==============================================================================
# SECTION 4: Window Functions
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Advanced Analytics with Window Functions
# MAGIC
# MAGIC Calculate running totals, rankings, and time-series analytics.
# COMMAND ----------
@dp.materialized_view(
    comment="Customer purchase history with metrics"
)
def gold_customer_analytics():
    """
    Calculate customer lifetime value and purchase patterns.
    
    Window functions:
    - Running total of purchases
    - Customer rank by spend
    - Days since last purchase
    """
    window_spec = Window.partitionBy("customer_id").orderBy("sale_date")
    
    return spark.read.table("silver_sales_enriched") \
        .withColumn(
            "cumulative_spend",
            sum("amount").over(window_spec)
        ) \
        .withColumn(
            "purchase_number",
            row_number().over(window_spec)
        ) \
        .withColumn(
            "days_since_previous_purchase",
            datediff(
                col("sale_date"),
                lag("sale_date", 1).over(window_spec)
            )
        ) \
        .select(
            "customer_id",
            "customer_name",
            "sale_date",
            "amount",
            "cumulative_spend",
            "purchase_number",
            "days_since_previous_purchase"
        )

# ==============================================================================
# SECTION 5: Complex Business Logic
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Implementing Business Rules
# MAGIC
# MAGIC Apply complex business logic and calculations.
# COMMAND ----------
@dp.materialized_view(
    comment="Customer segmentation based on RFM analysis"
)
def gold_customer_segments():
    """
    RFM (Recency, Frequency, Monetary) segmentation.
    
    Segments:
    - VIP: Recent, frequent, high-value customers
    - Regular: Moderate activity
    - At Risk: Not purchased recently
    - Dormant: Inactive for extended period
    """
    from datetime import datetime
    
    today = datetime.now()
    
    customer_summary = spark.read.table("silver_sales_enriched") \
        .groupBy("customer_id", "customer_name") \
        .agg(
            max("sale_date").alias("last_purchase_date"),
            count("sale_id").alias("purchase_count"),
            sum("amount").alias("total_spend")
        )
    
    return customer_summary \
        .withColumn(
            "days_since_purchase",
            datediff(lit(today), col("last_purchase_date"))
        ) \
        .withColumn(
            "segment",
            when((col("days_since_purchase") <= 30) & 
                 (col("purchase_count") >= 5) & 
                 (col("total_spend") >= 1000), "VIP")
            .when((col("days_since_purchase") <= 60) & 
                  (col("purchase_count") >= 3), "Regular")
            .when(col("days_since_purchase") <= 180, "At Risk")
            .otherwise("Dormant")
        ) \
        .select(
            "customer_id",
            "customer_name",
            "last_purchase_date",
            "days_since_purchase",
            "purchase_count",
            "total_spend",
            "segment"
        )

# ==============================================================================
# SECTION 6: Time-Based Aggregations
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Rolling Windows and Time Series
# MAGIC
# MAGIC Calculate moving averages and trends.
# COMMAND ----------
@dp.materialized_view(
    comment="7-day rolling average sales"
)
def gold_sales_trends():
    """
    Calculate rolling 7-day metrics.
    
    Metrics:
    - 7-day moving average revenue
    - Week-over-week growth
    - Trend indicator
    """
    window_7day = Window.orderBy("date").rowsBetween(-6, 0)
    
    daily = spark.read.table("gold_daily_sales")
    
    return daily \
        .withColumn(
            "revenue_7day_avg",
            avg("total_revenue").over(window_7day)
        ) \
        .withColumn(
            "revenue_7day_ago",
            lag("total_revenue", 7).over(Window.orderBy("date"))
        ) \
        .withColumn(
            "week_over_week_growth",
            (col("total_revenue") - col("revenue_7day_ago")) / col("revenue_7day_ago") * 100
        ) \
        .withColumn(
            "trend",
            when(col("week_over_week_growth") > 10, "Strong Growth")
            .when(col("week_over_week_growth") > 0, "Moderate Growth")
            .when(col("week_over_week_growth") > -10, "Slight Decline")
            .otherwise("Significant Decline")
        )

# ==============================================================================
# KEY CONCEPTS SUMMARY
# ==============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Materialized View Best Practices
# MAGIC
# MAGIC ### ✅ DO:
# MAGIC 1. Use standard spark.read.table() for batch processing
# MAGIC 2. Leverage engine's smart refresh decisions
# MAGIC 3. Use explicit column selection (avoid SELECT *)
# MAGIC 4. Cache intermediate results for complex logic
# MAGIC 5. Apply filters early in transformation chain
# MAGIC 6. Use broadcast joins for small dimension tables
# MAGIC
# MAGIC ### ❌ DON'T:
# MAGIC 1. Use streaming reads in materialized views
# MAGIC 2. Call .collect(), .count() inside definitions
# MAGIC 3. Mix streaming and batch reads
# MAGIC 4. Create circular dependencies between views
# MAGIC 5. Forget to optimize join order
# MAGIC
# MAGIC ### 💡 Pro Tips:
# MAGIC - Engine automatically decides full vs incremental refresh
# MAGIC - Views update only when upstream data changes
# MAGIC - Use PARTITION BY in window functions carefully (memory)
# MAGIC - Consider liquid clustering for large aggregations
# MAGIC - Monitor query plans with explain()
# MAGIC
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC **For Large Aggregations:**
# MAGIC - Use broadcast joins for dimension tables
# MAGIC - Partition data appropriately
# MAGIC - Enable adaptive query execution (AQE)
# MAGIC - Use Z-ORDER clustering on common filters
# MAGIC
# MAGIC **For Complex Joins:**
# MAGIC - Filter before joining
# MAGIC - Join on indexed columns when possible
# MAGIC - Broadcast small tables (<10GB)
# MAGIC - Consider bucketing for repeated joins
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to `04_data_quality_expectations.py` to learn about
# MAGIC enforcing data quality rules with @dp.expect decorators.
