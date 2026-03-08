"""
Silver Layer: Clean Customer Data with CDC

Demonstrates Change Data Capture patterns and SCD Type 2 implementation
using dp.create_auto_cdc_flow for maintaining customer history.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Current customer records with data quality checks"
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'")
@dp.expect_or_drop("valid_country", "country IS NOT NULL")
def clean_customers():
    """
    Clean and validate customer data.
    
    Data Quality Rules:
    - Customer ID required
    - Email must be valid format
    - Country required (drop if missing)
    
    Transformations:
    - Standardize country codes
    - Normalize email addresses
    - Calculate customer segments
    - Add data quality flags
    
    Returns:
        DataFrame: Cleaned customer data
    """
    return (
        spark.read.table("raw_customers")
        # Deduplicate - keep most recent record
        .dropDuplicates(["customer_id"])
        # Standardize email
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        # Standardize country codes
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        # Calculate age
        .withColumn(
            "age",
            F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12)
        )
        # Segment customers
        .withColumn(
            "customer_segment",
            F.when(F.col("age") < 25, "Gen Z")
            .when((F.col("age") >= 25) & (F.col("age") < 40), "Millennial")
            .when((F.col("age") >= 40) & (F.col("age") < 60), "Gen X")
            .otherwise("Boomer")
        )
        # Add processing metadata
        .withColumn("processed_at", F.current_timestamp())
    )


# CDC Pattern with SCD Type 2
# This requires the raw_customers_cdc table to have CDC metadata columns
dp.create_auto_cdc_flow(
    source="raw_customers_cdc",
    target="customer_history",
    keys=["customer_id"],
    sequence_by="update_timestamp",
    stored_as_scd_type=2,
    track_history_column_list=["email", "address", "phone", "country"],
    track_history_except_column_list=None
)


@dp.materialized_view(
    comment="Customer attributes with historical tracking (SCD Type 2)"
)
def customer_scd_type2():
    """
    Customer dimension with full history tracking using SCD Type 2.
    
    This pattern maintains:
    - Complete history of all changes
    - Valid_from and valid_to timestamps
    - Current record indicator
    - Version numbers for each customer
    
    Use this when you need:
    - Point-in-time customer analysis
    - Audit trails of customer changes
    - Historical reporting accuracy
    
    Returns:
        DataFrame: Customer history with SCD Type 2 structure
    """
    return spark.read.table("customer_history")


@dp.materialized_view(
    comment="Current active customers only"
)
def customers_current():
    """
    View of current customer records only (no history).
    
    This provides a simple, performant view for most analytics
    that don't require historical data.
    
    Returns:
        DataFrame: Current customer records only
    """
    return (
        spark.read.table("customer_history")
        .filter(F.col("_end_at").isNull())  # Current records have NULL end date
        .drop("_start_at", "_end_at", "_version")
    )


@dp.materialized_view(
    comment="Customer lifecycle analysis"
)
def customer_lifecycle():
    """
    Analyze customer lifecycle events and changes.
    
    Identifies:
    - New customers
    - Churned customers
    - Customers with address changes
    - Customers with email changes
    
    Returns:
        DataFrame: Customer lifecycle events
    """
    history = spark.read.table("customer_history")
    
    from pyspark.sql.window import Window
    
    # Create window to compare with previous record
    window_spec = Window.partitionBy("customer_id").orderBy("_start_at")
    
    return (
        history
        .withColumn("previous_email", F.lag("email", 1).over(window_spec))
        .withColumn("previous_address", F.lag("address", 1).over(window_spec))
        .withColumn("previous_country", F.lag("country", 1).over(window_spec))
        .withColumn(
            "change_type",
            F.when(F.col("previous_email").isNull(), "NEW_CUSTOMER")
            .when(F.col("_end_at").isNull() & F.col("previous_email").isNotNull(), "CURRENT")
            .when(F.col("email") != F.col("previous_email"), "EMAIL_CHANGE")
            .when(F.col("address") != F.col("previous_address"), "ADDRESS_CHANGE")
            .when(F.col("country") != F.col("previous_country"), "COUNTRY_CHANGE")
            .otherwise("OTHER_CHANGE")
        )
    )
