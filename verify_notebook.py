from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, window, avg, when
import os

# Initialize Spark Session
print("Initializing Spark Session...")
spark = SparkSession.builder.appName("RetailAnalyticsVerify").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

PROCESSED_DIR = 'data/processed_transactions'

# 1. Load Data
print("Loading processed data...")
if not os.path.exists(PROCESSED_DIR):
    print(f"Error: {PROCESSED_DIR} does not exist. Run prepare_data.py first.")
    exit(1)

df = spark.read.parquet(PROCESSED_DIR)
print("Total records:", df.count())

# 2. Analysis: Weekly Sales
print("Analyzing Weekly Sales...")
weekly_sales = df.groupBy(
    "customer_name",
    window(col("date"), "1 week")
).agg(
    _sum("price_paid").alias("total_spend")
)
weekly_sales.show(5)

# 3. Classification Function
def classify_entity(df, entity_col, entity_name_col, metric_col='price_paid'):
    # 1. Calculate weekly sales per entity
    weekly_entity_sales = df.groupBy(
        entity_col,
        entity_name_col,
        window(col("date"), "1 week")
    ).agg(
        _sum(metric_col).alias("weekly_sales")
    )
    
    # 2. Calculate average weekly sales per entity
    avg_weekly_sales = weekly_entity_sales.groupBy(entity_col, entity_name_col) \
        .agg(avg("weekly_sales").alias("avg_weekly_sales"))
    
    # 3. Determine thresholds (33rd and 66th percentiles)
    quantiles = avg_weekly_sales.approxQuantile("avg_weekly_sales", [0.33, 0.66], 0.01)
    low_threshold = quantiles[0]
    high_threshold = quantiles[1]
    
    print(f"Thresholds for {entity_name_col}: Slow < {low_threshold:.2f}, Fast > {high_threshold:.2f}")
    
    # 4. Classify
    classified_df = avg_weekly_sales.withColumn(
        "classification",
        when(col("avg_weekly_sales") > high_threshold, "Fast")
        .when(col("avg_weekly_sales") < low_threshold, "Slow")
        .otherwise("Medium")
    )
    
    return classified_df.orderBy(col("avg_weekly_sales").desc())

# Classify Products
print("\n--- Product Classification ---")
classified_products = classify_entity(df, "product_id", "product_name")
classified_products.show(10)

# Classify Shops
print("\n--- Shop Classification ---")
classified_shops = classify_entity(df, "shop_id", "shop_name")
classified_shops.show(10)

print("Verification complete.")
