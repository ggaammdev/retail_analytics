from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import os
import shutil

# Initialize Spark Session
print("Initializing Spark Session...")
spark = SparkSession.builder.appName("RetailAnalyticsETL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

DATA_DIR = '../data'
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed_transactions')

# 1. Load Data
print("Loading raw data...")
# Load CSVs
df_customers = spark.read.csv(os.path.join(DATA_DIR, 'customers.csv'), header=True, inferSchema=True)
df_products = spark.read.csv(os.path.join(DATA_DIR, 'products.csv'), header=True, inferSchema=True)
df_shops = spark.read.csv(os.path.join(DATA_DIR, 'shops.csv'), header=True, inferSchema=True)

# Rename columns
df_customers = df_customers.withColumnRenamed("name", "customer_name")
df_products = df_products.withColumnRenamed("name", "product_name")
df_shops = df_shops.withColumnRenamed("name", "shop_name")

# Load Transactions (JSONL)
df_transactions = spark.read.json(os.path.join(DATA_DIR, 'transactions.jsonl'))

# 2. Preprocessing
print("Preprocessing transactions...")
df_exploded = df_transactions.select(
    col("transaction_id"),
    col("date"),
    col("shop_id"),
    col("purchaser_id"),
    explode(col("cart")).alias("cart_item")
)

# Extract elements from cart_item
df_exploded = df_exploded.withColumn("product_id", col("cart_item")[0]) \
                         .withColumn("price_paid", col("cart_item")[1].cast("double")) \
                         .drop("cart_item")

# Convert date
df_exploded = df_exploded.withColumn("date", col("date").cast("timestamp"))

# 3. Merge Data
print("Merging data...")
df_merged = df_exploded.join(df_customers, df_exploded.purchaser_id == df_customers.customer_id, "left") \
                       .join(df_products, df_exploded.product_id == df_products.product_id, "left") \
                       .join(df_shops, df_exploded.shop_id == df_shops.shop_id, "left") \
                       .drop(df_customers.customer_id) \
                       .drop(df_products.product_id) \
                       .drop(df_shops.shop_id)

# Select relevant columns for analysis
df_final = df_merged.select(
    col("transaction_id"),
    col("date"),
    col("shop_id"),
    col("shop_name"),
    col("purchaser_id"),
    col("customer_name"),
    col("product_id"),
    col("product_name"),
    col("category"),
    col("price_paid")
)

# 4. Save to Parquet
print(f"Saving processed data to {PROCESSED_DIR}...")
# Clean up existing processed data if any
if os.path.exists(PROCESSED_DIR):
    shutil.rmtree(PROCESSED_DIR)

df_final.write.parquet(PROCESSED_DIR)

print("ETL complete.")
