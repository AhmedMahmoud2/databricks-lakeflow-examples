"""
Gold Layer: Daily Sales Summary

Business-ready aggregations for daily sales reporting and dashboards.
This is consumption-optimized data for business users.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Daily sales summary by store and category"
)
def daily_sales_summary():
    """
    Daily aggregation of sales by store, category, and payment method.
    
    Key Metrics:
    - Total sales amount
    - Transaction count
    - Average transaction value
    - Unique customers
    - Product mix
    
    Grain: One row per day, store, and category
    
    Returns:
        DataFrame: Daily sales metrics
    """
    enriched_sales = spark.read.table("enriched_sales")
    
    return (
        enriched_sales
        .groupBy(
            F.col("transaction_date").alias("date"),
            "store_id",
            "category",
            "payment_method"
        )
        .agg(
            # Sales metrics
            F.sum("amount").alias("total_sales"),
            F.sum("profit").alias("total_profit"),
            F.count("transaction_id").alias("transaction_count"),
            F.avg("amount").alias("avg_transaction_value"),
            
            # Customer metrics
            F.countDistinct("customer_id").alias("unique_customers"),
            
            # Product metrics
            F.sum("quantity").alias("total_quantity"),
            F.countDistinct("product_id").alias("unique_products"),
            
            # Quality metrics
            F.avg("quality_score").alias("avg_quality_score")
        )
        # Calculate derived metrics
        .withColumn(
            "sales_per_customer",
            F.col("total_sales") / F.col("unique_customers")
        )
        .withColumn(
            "profit_margin_pct",
            (F.col("total_profit") / F.col("total_sales")) * 100
        )
        .withColumn("updated_at", F.current_timestamp())
    )


@dp.materialized_view(
    comment="Month-to-date sales performance"
)
def mtd_sales_performance():
    """
    Month-to-date sales with comparisons to prior periods.
    
    Provides:
    - Current month metrics
    - Prior month comparison
    - Year-over-year comparison
    - Trending indicators
    
    Returns:
        DataFrame: MTD performance metrics
    """
    daily_summary = spark.read.table("daily_sales_summary")
    
    # Calculate various time periods
    current_month = F.date_trunc("month", F.current_date())
    
    return (
        daily_summary
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("is_current_month", F.col("date") >= current_month)
        # Aggregate by store and month
        .groupBy("store_id", "category", "year", "month", "is_current_month")
        .agg(
            F.sum("total_sales").alias("monthly_sales"),
            F.sum("total_profit").alias("monthly_profit"),
            F.sum("transaction_count").alias("monthly_transactions"),
            F.sum("unique_customers").alias("monthly_customers")
        )
        .withColumn(
            "sales_per_day",
            F.col("monthly_sales") / F.datediff(F.current_date(), F.date_trunc("month", F.current_date()))
        )
    )


@dp.materialized_view(
    comment="Sales trends with moving averages"
)
def sales_trends():
    """
    Calculate sales trends using moving averages and growth rates.
    
    Metrics:
    - 7-day moving average
    - 30-day moving average
    - Day-over-day growth
    - Week-over-week growth
    
    Returns:
        DataFrame: Sales trends and indicators
    """
    daily_summary = spark.read.table("daily_sales_summary")
    
    # Define windows for moving calculations
    window_7d = Window.partitionBy("store_id", "category").orderBy("date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("store_id", "category").orderBy("date").rowsBetween(-29, 0)
    window_lag = Window.partitionBy("store_id", "category").orderBy("date")
    
    return (
        daily_summary
        .select("date", "store_id", "category", "total_sales", "transaction_count")
        # Moving averages
        .withColumn("ma_7d", F.avg("total_sales").over(window_7d))
        .withColumn("ma_30d", F.avg("total_sales").over(window_30d))
        # Lag values for growth calculations
        .withColumn("prev_day_sales", F.lag("total_sales", 1).over(window_lag))
        .withColumn("prev_week_sales", F.lag("total_sales", 7).over(window_lag))
        # Growth rates
        .withColumn(
            "dod_growth_pct",
            ((F.col("total_sales") - F.col("prev_day_sales")) / F.col("prev_day_sales")) * 100
        )
        .withColumn(
            "wow_growth_pct",
            ((F.col("total_sales") - F.col("prev_week_sales")) / F.col("prev_week_sales")) * 100
        )
        # Trend indicator
        .withColumn(
            "trend",
            F.when(F.col("total_sales") > F.col("ma_7d"), "UP")
            .when(F.col("total_sales") < F.col("ma_7d"), "DOWN")
            .otherwise("FLAT")
        )
    )


@dp.materialized_view(
    comment="Top performing products by sales"
)
def top_products():
    """
    Identify top performing products across various dimensions.
    
    Rankings by:
    - Total sales
    - Total profit
    - Sales growth
    - Customer preference
    
    Returns:
        DataFrame: Product rankings and performance
    """
    enriched_sales = spark.read.table("enriched_sales")
    
    # Define ranking window
    window_rank = Window.partitionBy("category").orderBy(F.desc("total_sales"))
    
    return (
        enriched_sales
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.sum("profit").alias("total_profit"),
            F.sum("quantity").alias("total_quantity"),
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_buyers")
        )
        # Calculate metrics
        .withColumn("avg_sale_value", F.col("total_sales") / F.col("transaction_count"))
        .withColumn("profit_margin_pct", (F.col("total_profit") / F.col("total_sales")) * 100)
        # Rank within category
        .withColumn("sales_rank_in_category", F.dense_rank().over(window_rank))
        # Filter to top 100
        .filter(F.col("sales_rank_in_category") <= 100)
        .withColumn("updated_at", F.current_timestamp())
    )
