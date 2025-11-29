from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, window, avg, when, countDistinct
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

# 2. Classification Function (Needed for Q3/Q4)
def classify_entity(df, entity_col, entity_name_col, metric_col='price_paid'):
    weekly_entity_sales = df.groupBy(entity_col, entity_name_col, window(col("date"), "1 week")).agg(_sum(metric_col).alias("weekly_sales"))
    avg_weekly_sales = weekly_entity_sales.groupBy(entity_col, entity_name_col).agg(avg("weekly_sales").alias("avg_weekly_sales"))
    quantiles = avg_weekly_sales.approxQuantile("avg_weekly_sales", [0.33, 0.66], 0.01)
    low_threshold, high_threshold = quantiles[0], quantiles[1]
    
    classified_df = avg_weekly_sales.withColumn(
        "classification",
        when(col("avg_weekly_sales") > high_threshold, "Fast")
        .when(col("avg_weekly_sales") < low_threshold, "Slow")
        .otherwise("Medium")
    )
    return classified_df

print("Classifying entities...")
classified_products = classify_entity(df, "product_id", "product_name")
classified_shops = classify_entity(df, "shop_id", "shop_name")

# 3. Promotion Impact Analysis
print("Analyzing Promotion Impact...")

# Flag Transactions
df_with_promo = df.alias("t").join(
    df_promotions.alias("p"),
    (col("t.product_id") == col("p.product_id")) &
    (col("t.date") >= col("p.start_date")) &
    (col("t.date") <= col("p.end_date")),
    "left"
).select(
    col("t.*"),
    when(col("p.promotion_id").isNotNull(), 1).otherwise(0).alias("is_promo")
)

# Helper function to calculate lift
def calculate_lift(df_input, group_cols):
    stats = df_input.groupBy(group_cols + ["is_promo"]).agg(
        _sum("price_paid").alias("total_sales"),
        countDistinct("date").alias("days_active")
    )
    regular = stats.filter(col("is_promo") == 0).withColumnRenamed("total_sales", "reg_sales").withColumnRenamed("days_active", "reg_days")
    promo = stats.filter(col("is_promo") == 1).withColumnRenamed("total_sales", "promo_sales").withColumnRenamed("days_active", "promo_days")
    
    joined = regular.join(promo, group_cols, "left")
    joined = joined.withColumn("avg_daily_reg", col("reg_sales") / col("reg_days")) \
                   .withColumn("avg_daily_promo", col("promo_sales") / col("promo_days"))
    
    joined = joined.withColumn("lift", (col("avg_daily_promo") - col("avg_daily_reg")) / col("avg_daily_reg"))
    return joined.orderBy(col("lift").desc())

# Q1: Item Sales Lift
print("\n--- Q1: Item Sales Lift (Top 5) ---")
item_lift = calculate_lift(df_with_promo, ["product_id", "product_name"])
item_lift.select("product_name", "avg_daily_reg", "avg_daily_promo", "lift").show(5)

# Q2: Store Sales Lift
print("\n--- Q2: Store Sales Lift (Top 5) ---")
store_lift = calculate_lift(df_with_promo, ["shop_id", "shop_name"])
store_lift.select("shop_name", "avg_daily_reg", "avg_daily_promo", "lift").show(5)

# Q3: Fast vs Slow Items Impact
print("\n--- Q3: Fast vs Slow Items Impact ---")
item_impact = item_lift.join(classified_products.select("product_id", "classification"), "product_id")
item_impact.groupBy("classification").agg(avg("lift").alias("avg_lift")).orderBy("avg_lift").show()

# Q4: Fast vs Slow Stores Impact
print("\n--- Q4: Fast vs Slow Stores Impact ---")
store_impact = store_lift.join(classified_shops.select("shop_id", "classification"), "shop_id")
store_impact.groupBy("classification").agg(avg("lift").alias("avg_lift")).orderBy("avg_lift").show()

print("Verification complete.")
