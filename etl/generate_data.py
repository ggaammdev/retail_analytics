import pandas as pd
import numpy as np
import random
import json
import os
from datetime import datetime, timedelta

# Configuration
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_SHOPS = 5
NUM_TRANSACTIONS = 500
DATA_DIR = '../data'

os.makedirs(DATA_DIR, exist_ok=True)

# 1. Generate Customers
print("Generating Customers...")
customers = []
for i in range(NUM_CUSTOMERS):
    customers.append({
        'customer_id': f'C{i:03d}',
        'name': f'Customer_{i}',
        'email': f'customer_{i}@example.com',
        'join_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
    })
df_customers = pd.DataFrame(customers)
df_customers.to_csv(os.path.join(DATA_DIR, 'customers.csv'), index=False)

# 2. Generate Products
print("Generating Products...")
categories = ['Electronics', 'Clothing', 'Home', 'Books', 'Sports']
products = []
for i in range(NUM_PRODUCTS):
    products.append({
        'product_id': f'P{i:03d}',
        'category': random.choice(categories),
        'name': f'Product_{i}',
        'base_price': round(random.uniform(10, 500), 2)
    })
df_products = pd.DataFrame(products)
df_products.to_csv(os.path.join(DATA_DIR, 'products.csv'), index=False)

# 3. Generate Shops
print("Generating Shops...")
shops = []
locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
for i in range(NUM_SHOPS):
    shops.append({
        'shop_id': f'S{i:03d}',
        'location': locations[i % len(locations)],
        'name': f'Shop_{locations[i % len(locations)]}'
    })
df_shops = pd.DataFrame(shops)
df_shops.to_csv(os.path.join(DATA_DIR, 'shops.csv'), index=False)

# 4. Generate Promotions
print("Generating Promotions...")
start_date = datetime.now() - timedelta(days=90)
promotions = []
# Create ~20 promotions
for i in range(20):
    prod = random.choice(products)
    promo_start = start_date + timedelta(days=random.randint(0, 60))
    duration = random.randint(3, 14) # 3 to 14 days
    promo_end = promo_start + timedelta(days=duration)
    discount = random.choice([0.10, 0.20, 0.25, 0.50])
    
    promotions.append({
        'promotion_id': f'PROMO{i:03d}',
        'product_id': prod['product_id'],
        'start_date': promo_start.strftime('%Y-%m-%d'),
        'end_date': promo_end.strftime('%Y-%m-%d'),
        'discount_percentage': discount
    })
df_promotions = pd.DataFrame(promotions)
df_promotions.to_csv(os.path.join(DATA_DIR, 'promotions.csv'), index=False)

# Helper to check for active promotion
def get_discounted_price(product_id, base_price, date_obj):
    for promo in promotions:
        p_start = datetime.strptime(promo['start_date'], '%Y-%m-%d')
        p_end = datetime.strptime(promo['end_date'], '%Y-%m-%d')
        if promo['product_id'] == product_id and p_start <= date_obj <= p_end:
            return round(base_price * (1 - promo['discount_percentage']), 2)
    return base_price

# 5. Generate Transactions
print("Generating Transactions...")
transactions = []

for i in range(NUM_TRANSACTIONS):
    tx_date = start_date + timedelta(days=random.randint(0, 90))
    
    # 10% chance of being a return
    is_return = random.random() < 0.1
    
    # Random cart
    cart_size = random.randint(1, 5)
    cart = []
    for _ in range(cart_size):
        prod = random.choice(products)
        
        # Apply promotion if active
        price = get_discounted_price(prod['product_id'], prod['base_price'], tx_date)
        
        # Small random variation still applies
        price = round(price * random.uniform(0.95, 1.05), 2)
        
        if is_return:
            price = -price
        cart.append((prod['product_id'], price))
    
    transactions.append({
        'transaction_id': f'T{i:05d}',
        'date': tx_date.strftime('%Y-%m-%d %H:%M:%S'),
        'shop_id': random.choice(shops)['shop_id'],
        'purchaser_id': random.choice(customers)['customer_id'],
        'cart': cart
    })

# Save as JSON lines because 'cart' is a nested structure
with open(os.path.join(DATA_DIR, 'transactions.jsonl'), 'w') as f:
    for tx in transactions:
        f.write(json.dumps(tx) + '\n')

print("Data generation complete.")
