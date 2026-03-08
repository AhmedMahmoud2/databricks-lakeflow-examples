# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 05 - Complete Medallion Architecture
# MAGIC
# MAGIC ## Overview
# MAGIC Build a production-ready Bronze → Silver → Gold pipeline.
# MAGIC See how automatic dependency inference and incremental processing work together.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC Bronze (Raw)           Silver (Clean)         Gold (Business)
# MAGIC ━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━
# MAGIC raw_customers      →   clean_customers    →   customer_360
# MAGIC raw_orders         →   clean_orders       →   daily_revenue
# MAGIC raw_products       →   clean_products     →   product_performance
# MAGIC                        enriched_sales     →   sales_dashboard
# MAGIC
# MAGIC ## Author  
# MAGIC Ahmed Mahmoud - DataMindAI
# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ==============================================================================
# BRONZE LAYER - Raw Ingestion
# ==============================================================================

@dp.table(comment="Raw customers from S3", table_properties={"quality": "bronze"})
def bronze_customers():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/customers")

@dp.table(comment="Raw orders from S3", table_properties={"quality": "bronze"})
def bronze_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/orders")

@dp.table(comment="Raw products from S3", table_properties={"quality": "bronze"})
def bronze_products():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/raw/products")

# ==============================================================================
# SILVER LAYER - Cleaned & Validated
# ==============================================================================

@dp.materialized_view(comment="Cleaned customers")
@dp.expect_or_drop("valid_email", "email RLIKE '.+@.+\\..+'")
@dp.expect_or_drop("valid_country", "country IN ('US', 'UK', 'CA', 'AU')")
def silver_customers():
    return spark.read.table("bronze_customers") \
        .filter(col("customer_id").isNotNull()) \
        .dropDuplicates(["customer_id"]) \
        .select("customer_id", "first_name", "last_name", 
                lower(col("email")).alias("email"), "country")

@dp.materialized_view(comment="Cleaned orders")
@dp.expect("positive_amount", "total_amount > 0")
@dp.expect_or_drop("valid_dates", "order_date <= CURRENT_DATE()")
def silver_orders():
    return spark.read.table("bronze_orders") \
        .filter(col("order_id").isNotNull()) \
        .select("order_id", "customer_id", "product_id", 
                "quantity", "total_amount", "order_date")

@dp.materialized_view(comment="Cleaned products")
def silver_products():
    return spark.read.table("bronze_products") \
        .filter(col("product_id").isNotNull()) \
        .dropDuplicates(["product_id"]) \
        .select("product_id", "product_name", "category", "unit_price")

# ==============================================================================
# SILVER LAYER - Enriched (Joins)
# ==============================================================================

@dp.materialized_view(comment="Orders enriched with customer and product info")
def silver_orders_enriched():
    orders = spark.read.table("silver_orders")
    customers = spark.read.table("silver_customers")
    products = spark.read.table("silver_products")
    
    return orders \
        .join(customers, "customer_id") \
        .join(products, "product_id") \
        .select(
            orders["*"],
            customers["first_name"], customers["last_name"], customers["country"],
            products["product_name"], products["category"]
        )

# ==============================================================================
# GOLD LAYER - Business Metrics
# ==============================================================================

@dp.materialized_view(comment="Customer 360 view")
def gold_customer_360():
    return spark.read.table("silver_orders_enriched") \
        .groupBy("customer_id", "first_name", "last_name", "country") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_value"),
            max("order_date").alias("last_order_date")
        )

@dp.materialized_view(comment="Daily revenue dashboard")
def gold_daily_revenue():
    return spark.read.table("silver_orders_enriched") \
        .groupBy(to_date("order_date").alias("date")) \
        .agg(
            count("order_id").alias("orders"),
            sum("total_amount").alias("revenue"),
            countDistinct("customer_id").alias("customers")
        )

@dp.materialized_view(comment="Product performance metrics")
def gold_product_performance():
    return spark.read.table("silver_orders_enriched") \
        .groupBy("product_id", "product_name", "category") \
        .agg(
            sum("quantity").alias("units_sold"),
            sum("total_amount").alias("revenue"),
            count("order_id").alias("order_count")
        )
# COMMAND ----------
# MAGIC %md
# MAGIC ## How the DAG Works
# MAGIC
# MAGIC The engine automatically infers:
# MAGIC
# MAGIC bronze_customers ──┐
# MAGIC                    ├──> silver_customers ──┐
# MAGIC bronze_orders ─────┤                       ├──> silver_orders_enriched ──┬──> gold_customer_360
# MAGIC                    ├──> silver_orders ─────┘                             │
# MAGIC bronze_products ───┴──> silver_products ───────────────────────────────┼──> gold_daily_revenue
# MAGIC                                                                          └──> gold_product_performance
# MAGIC
# MAGIC Dependencies are determined by reading spark.read.table() references!
# MAGIC
# MAGIC ## Key Benefits
# MAGIC
# MAGIC 1. No manual DAG definition
# MAGIC 2. Automatic incremental processing
# MAGIC 3. Only recompute when upstream changes
# MAGIC 4. Full lineage through Unity Catalog
# MAGIC 5. Optimized resource allocation
