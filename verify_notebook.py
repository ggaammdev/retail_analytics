from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, window, avg, when, countDistinct, count, date_format, row_number
from pyspark.sql.window import Window
import os

# Initialize Spark Session
print("Initializing Spark Session...")
spark = SparkSession.builder.appName("RetailAnalyticsVerify").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

PROCESSED_DIR = 'data/processed_transactions'
PROMO_PROCESSED_DIR = 'data/processed_promotions'

# 1. Load Data
print("Loading processed data...")
if not os.path.exists(PROCESSED_DIR) or not os.path.exists(PROMO_PROCESSED_DIR):
    print(f"Error: Data directories do not exist. Run prepare_data.py first.")
    exit(1)

df = spark.read.parquet(PROCESSED_DIR)
df_promotions = spark.read.parquet(PROMO_PROCESSED_DIR)

# ... (Previous logic for Q1-Q4 omitted for brevity in this verification update, focusing on new Qs) ...
# Note: In a real scenario, I'd keep Q1-Q4 or make them modular. 
# For this verification, I will include Q5-Q8 logic specifically.

print("\n--- Expanded Analysis Verification (Q5-Q8) ---")

# Q5: Return Rate Analysis by Category
print("\n--- Q5: Return Rate by Category ---")
category_stats = df.groupBy("category").agg(
    count("transaction_id").alias("total_txns"),
    _sum(when(col("price_paid") < 0, 1).otherwise(0)).alias("return_count")
)
category_returns = category_stats.withColumn(
    "return_rate", col("return_count") / col("total_txns")
).orderBy(col("return_rate").desc())
category_returns.show()

# Q6: Customer Segmentation (Frequency & Monetary)
print("\n--- Q6: Top Customers (Frequency & Monetary) ---")
customer_metrics = df.groupBy("customer_name").agg(
    countDistinct("transaction_id").alias("frequency"),
    _sum("price_paid").alias("monetary")
).orderBy(col("monetary").desc())
customer_metrics.show(5)

# Q7: Geographic Preferences (Top Category per Store)
print("\n--- Q7: Top Category per Store ---")
store_category_sales = df.groupBy("shop_name", "category").agg(
    _sum("price_paid").alias("total_sales")
)
window_spec = Window.partitionBy("shop_name").orderBy(col("total_sales").desc())
top_categories = store_category_sales.withColumn(
    "rank", row_number().over(window_spec)
).filter(col("rank") == 1).drop("rank")
top_categories.orderBy("shop_name").show()

# Q8: Peak Trading Times (Busiest Day of Week)
print("\n--- Q8: Busiest Day of Week ---")
df_with_day = df.withColumn("day_of_week", date_format(col("date"), "E"))
daily_volume = df_with_day.groupBy("day_of_week").agg(
    count("transaction_id").alias("txn_count")
).orderBy(col("txn_count").desc())
daily_volume.show()

print("Verification complete.")
