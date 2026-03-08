"""
Gold Layer: Customer Metrics

Customer analytics and KPIs for business intelligence dashboards.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Customer lifetime value and behavior metrics"
)
def customer_metrics():
    """
    Calculate customer-level metrics for segmentation and targeting.
    
    Key Metrics:
    - Customer Lifetime Value (CLV)
    - Recency, Frequency, Monetary (RFM)
    - Average Order Value (AOV)
    - Purchase frequency
    - Churn indicators
    
    Returns:
        DataFrame: Customer-level analytics
    """
    enriched_sales = spark.read.table("enriched_sales")
    customers_current = spark.read.table("customers_current")
    
    # Calculate customer purchase metrics
    customer_purchases = (
        enriched_sales
        .groupBy("customer_id")
        .agg(
            # Monetary
            F.sum("amount").alias("lifetime_value"),
            F.avg("amount").alias("avg_order_value"),
            F.sum("profit").alias("lifetime_profit"),
            
            # Frequency
            F.count("transaction_id").alias("total_purchases"),
            F.countDistinct("transaction_date").alias("purchase_days"),
            F.countDistinct("product_id").alias("unique_products_bought"),
            F.countDistinct("category").alias("unique_categories_bought"),
            
            # Recency
            F.max("transaction_date").alias("last_purchase_date"),
            F.min("transaction_date").alias("first_purchase_date"),
            
            # Behavior
            F.collect_set("payment_method").alias("payment_methods_used")
        )
    )
    
    # Calculate derived metrics
    result = (
        customer_purchases
        .withColumn(
            "days_since_last_purchase",
            F.datediff(F.current_date(), F.col("last_purchase_date"))
        )
        .withColumn(
            "customer_tenure_days",
            F.datediff(F.current_date(), F.col("first_purchase_date"))
        )
        .withColumn(
            "purchase_frequency",
            F.col("total_purchases") / F.col("customer_tenure_days")
        )
        # RFM Score calculation
        .withColumn("recency_score", 
            F.when(F.col("days_since_last_purchase") <= 30, 5)
            .when(F.col("days_since_last_purchase") <= 60, 4)
            .when(F.col("days_since_last_purchase") <= 90, 3)
            .when(F.col("days_since_last_purchase") <= 180, 2)
            .otherwise(1)
        )
        .withColumn("frequency_score",
            F.when(F.col("total_purchases") >= 20, 5)
            .when(F.col("total_purchases") >= 10, 4)
            .when(F.col("total_purchases") >= 5, 3)
            .when(F.col("total_purchases") >= 2, 2)
            .otherwise(1)
        )
        .withColumn("monetary_score",
            F.when(F.col("lifetime_value") >= 5000, 5)
            .when(F.col("lifetime_value") >= 2000, 4)
            .when(F.col("lifetime_value") >= 1000, 3)
            .when(F.col("lifetime_value") >= 500, 2)
            .otherwise(1)
        )
        .withColumn(
            "rfm_score",
            F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score")
        )
        # Customer segment
        .withColumn(
            "customer_segment",
            F.when(F.col("rfm_score") >= 13, "VIP")
            .when(F.col("rfm_score") >= 10, "LOYAL")
            .when(F.col("rfm_score") >= 7, "POTENTIAL")
            .when(F.col("rfm_score") >= 4, "AT_RISK")
            .otherwise("LOST")
        )
    )
    
    # Join with customer demographics
    return (
        result.alias("m")
        .join(customers_current.alias("c"), "customer_id", "left")
        .select(
            "m.*",
            "c.customer_segment as demographic_segment",
            "c.age",
            "c.country"
        )
        .withColumn("updated_at", F.current_timestamp())
    )


@dp.materialized_view(
    comment="Customer cohort analysis by registration month"
)
def customer_cohorts():
    """
    Cohort analysis showing customer retention and behavior over time.
    
    Analyzes:
    - Cohort retention rates
    - Cohort lifetime value
    - Cohort purchase patterns
    
    Returns:
        DataFrame: Customer cohort metrics
    """
    enriched_sales = spark.read.table("enriched_sales")
    customers = spark.read.table("customers_current")
    
    # Determine customer cohort (first purchase month)
    first_purchase = (
        enriched_sales
        .groupBy("customer_id")
        .agg(
            F.min("transaction_date").alias("first_purchase_date"),
            F.date_trunc("month", F.min("transaction_date")).alias("cohort_month")
        )
    )
    
    # Join sales with cohort information
    sales_with_cohort = (
        enriched_sales.alias("s")
        .join(first_purchase.alias("fp"), "customer_id")
        .withColumn("months_since_first_purchase",
            F.months_between(F.col("transaction_date"), F.col("first_purchase_date"))
        )
    )
    
    # Aggregate by cohort and time period
    return (
        sales_with_cohort
        .groupBy("cohort_month", "months_since_first_purchase")
        .agg(
            F.countDistinct("customer_id").alias("active_customers"),
            F.sum("amount").alias("cohort_revenue"),
            F.avg("amount").alias("avg_purchase_value"),
            F.count("transaction_id").alias("total_transactions")
        )
        .withColumn(
            "avg_revenue_per_customer",
            F.col("cohort_revenue") / F.col("active_customers")
        )
        .withColumn("updated_at", F.current_timestamp())
    )


@dp.materialized_view(
    comment="Churn prediction indicators"
)
def churn_indicators():
    """
    Calculate churn risk indicators for proactive retention.
    
    Risk Factors:
    - Long time since last purchase
    - Decreasing purchase frequency
    - Declining order values
    - Reduced category engagement
    
    Returns:
        DataFrame: Churn risk scores and indicators
    """
    customer_metrics_df = spark.read.table("customer_metrics")
    
    return (
        customer_metrics_df
        .withColumn(
            "churn_risk_score",
            # Calculate risk score (0-100)
            F.when(F.col("days_since_last_purchase") > 180, 80)
            .when(F.col("days_since_last_purchase") > 120, 60)
            .when(F.col("days_since_last_purchase") > 90, 40)
            .when(F.col("days_since_last_purchase") > 60, 20)
            .otherwise(10)
            +
            F.when(F.col("purchase_frequency") < 0.01, 20)  # Less than once per 100 days
            .when(F.col("purchase_frequency") < 0.05, 10)
            .otherwise(0)
        )
        .withColumn(
            "churn_risk_category",
            F.when(F.col("churn_risk_score") >= 70, "HIGH")
            .when(F.col("churn_risk_score") >= 40, "MEDIUM")
            .otherwise("LOW")
        )
        .withColumn(
            "days_until_likely_churn",
            F.when(F.col("churn_risk_score") >= 70, 30)
            .when(F.col("churn_risk_score") >= 40, 60)
            .otherwise(120)
        )
        .withColumn(
            "recommended_action",
            F.when(F.col("churn_risk_category") == "HIGH", "URGENT: Send win-back offer")
            .when(F.col("churn_risk_category") == "MEDIUM", "ENGAGE: Send personalized email")
            .otherwise("MAINTAIN: Regular newsletter")
        )
        .select(
            "customer_id",
            "customer_segment",
            "lifetime_value",
            "days_since_last_purchase",
            "purchase_frequency",
            "churn_risk_score",
            "churn_risk_category",
            "days_until_likely_churn",
            "recommended_action",
            "updated_at"
        )
    )
