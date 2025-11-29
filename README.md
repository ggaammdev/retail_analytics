# Retail Analytics Project

This project implements a retail analytics pipeline using **PySpark**. It generates synthetic data, performs ETL (Extract, Transform, Load) to prepare the data, and runs analysis to answer business questions and classify entities.

## Project Structure

- **`etl/generate_data.py`**: Python script to generate synthetic data for Customers, Products, Shops, and Transactions.
- **`etl/prepare_data.py`**: PySpark ETL script. It reads the raw CSV/JSONL data, joins the tables, cleans the data, and saves the result as Parquet files in `data/processed_transactions`.
- **`retail_analytics.ipynb`**: Jupyter Notebook containing the core analysis. It loads the processed Parquet data and answers business questions.
- **`verify_notebook.py`**: A standalone Python script to verify the logic of the notebook without launching Jupyter.
- **`data/`**: Directory containing generated raw data and the `processed_transactions` Parquet folder.

## Implementations

### 1. Data Generation
Synthetic data is generated using `pandas` and `faker`-like logic (random selection).
- **Customers**: 100 profiles.
- **Products**: 50 items across 5 categories.
- **Shops**: 5 store locations.
- **Transactions**: 500 transactions with nested cart items (product_id, price).
    - **Sales**: Regular purchase transactions.
    - **Returns**: ~10% of transactions are generated as returns (negative prices), allowing for net sales analysis.
    - **Promotions**: A `promotions` dataset is generated. Transactions occurring during promotion periods automatically reflect discounted prices.

### 2. ETL (Data Preparation)
The `prepare_data.py` script uses PySpark to:
- Load raw CSVs (Customers, Products, Shops) and JSONL (Transactions).
- Explode the nested `cart` arrays in transactions to create individual line items.
- Join transactions with Customer, Product, and Shop details.
- Save the enriched dataset to `data/processed_transactions` in Parquet format for efficient querying.

### 3. Analysis
The `retail_analytics.ipynb` notebook uses PySpark to answer business questions based on the processed data.
- **Weekly Sales**: Customer purchase trends.
- **Classification**: Fast/Medium/Slow items and stores.
- **Promotion Impact**: Analysis of sales lift during promotions and comparison across Fast/Slow entities.

## How to Run

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Generate Data**:
    ```bash
    cd etl
    python3 generate_data.py
    cd ..
    ```
2.  **Run ETL**:
    ```bash
    cd etl
    python3 prepare_data.py
    cd ..
    ```
3.  **Run Analysis**:
    Open `retail_analytics.ipynb` in Jupyter/VS Code, or run the verification script:
    ```bash
    python3 verify_notebook.py
    ```
