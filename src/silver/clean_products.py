"""
Silver Layer: Clean Product Data

Product master data with validation and enrichment.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Cleaned product catalog with hierarchies"
)
@dp.expect("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_price", "unit_price > 0")
@dp.expect_or_drop("valid_category", "category IS NOT NULL")
def clean_products():
    """
    Clean and validate product catalog data.
    
    Data Quality Rules:
    - Product ID required
    - Price must be positive
    - Category required (drop if missing)
    
    Transformations:
    - Standardize product names
    - Build product hierarchy
    - Calculate margin percentages
    - Add product flags
    
    Returns:
        DataFrame: Cleaned product data
    """
    return (
        spark.read.table("raw_products")
        # Deduplicate
        .dropDuplicates(["product_id"])
        # Standardize text fields
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.upper(F.trim(F.col("category"))))
        .withColumn("brand", F.trim(F.col("brand")))
        # Calculate margin
        .withColumn(
            "margin_pct",
            ((F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price")) * 100
        )
        # Flag high/low margin products
        .withColumn(
            "margin_category",
            F.when(F.col("margin_pct") > 50, "HIGH")
            .when(F.col("margin_pct") > 25, "MEDIUM")
            .otherwise("LOW")
        )
        # Active product flag
        .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True)))
        # Add processing timestamp
        .withColumn("processed_at", F.current_timestamp())
    )


@dp.materialized_view(
    comment="Product hierarchy for reporting"
)
def product_hierarchy():
    """
    Create product hierarchy for drill-down analysis.
    
    Hierarchy levels:
    - Division
    - Category  
    - Subcategory
    - Product
    
    Returns:
        DataFrame: Product hierarchy structure
    """
    return (
        spark.read.table("clean_products")
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("brand"),
            # Level 1: Division (derived from category)
            F.when(F.col("category").isin(["ELECTRONICS", "COMPUTERS"]), "TECHNOLOGY")
            .when(F.col("category").isin(["CLOTHING", "SHOES"]), "APPAREL")
            .when(F.col("category").isin(["FOOD", "BEVERAGES"]), "GROCERY")
            .otherwise("OTHER")
            .alias("division"),
            # Level 2: Category
            F.col("category"),
            # Level 3: Subcategory
            F.col("subcategory"),
            # Attributes
            F.col("unit_price"),
            F.col("unit_cost"),
            F.col("margin_pct"),
            F.col("is_active")
        )
    )
