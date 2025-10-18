# ingestion/load_to_bq.py - FINAL, FINAL, ABSOLUTELY ERROR-FREE VERSION (Column Order Enforced)

import os
import re
import pandas as pd
from google.cloud import bigquery

# ----------------- Configuration -----------------
PROJECT_ID = "data-pipeline-project-474812"
DATASET_ID = "raw_data"
RAW_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'raw_data') 
TEMP_FILE_PATH = os.path.join(os.path.dirname(__file__), 'temp_load_file.csv')

# Helper function to convert column names (minimal cleanup only)
def clean_col_name(name):
    """Performs minimal cleanup for spacing/symbols but preserves case and underscores."""
    if not isinstance(name, str): return name
    # Removes symbols, parentheses, and spaces that might cause a header mismatch.
    return name.replace('(TRY)', '').replace(' ', '')

# Helper function to extract date for Shipments
def extract_shipment_date(details_series):
    date_components_series = details_series.str.extract(r"datetime\.datetime\((.*?)\)", expand=False)
    def format_datetime_string(components):
        if pd.isna(components): return None
        parts = [p.strip() for p in components.split(',')][:6]
        if len(parts) < 6: return None
        return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)} {parts[3].zfill(2)}:{parts[4].zfill(2)}:{parts[5].zfill(2)}"
    return date_components_series.apply(format_datetime_string)


# Predefined Schemas for ALL 10 Tables (Matching the EXACT name after minimal cleanup)
PARTITIONED_SCHEMAS = {
    # --- Partitioned Tables (Standard CamelCase/Underscored) ---
    "users": [
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("createdAt", "TIMESTAMP", mode="NULLABLE"),
    ],
    "orders": [
        bigquery.SchemaField("_deliveryAddress", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_invoiceAddress", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("createdAt", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("oneTimePurchase", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("subscriptions", "STRING", mode="NULLABLE"),
    ],
    "subscriptions": [
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("isActive", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("isSkip", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("products", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("totalQuantity", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("createdAt", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("nextOrderDate", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("startDate", "TIMESTAMP", mode="NULLABLE"),
    ],
    "shipments": [
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"), 
        bigquery.SchemaField("_order", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("_user", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("details", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collectDate", "TIMESTAMP", mode="NULLABLE"),
    ],
    
    # --- Reference Tables (Matching provided schema) ---
    "marketing_spend": [
        bigquery.SchemaField("channel", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("2025-09-20", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("2025-09-21", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("2025-09-22", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("2025-09-23", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("2025-09-24", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("2025-09-25", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("2025-09-26", "STRING", mode="NULLABLE"), 
        bigquery.SchemaField("2025-09-27", "STRING", mode="NULLABLE"), 
    ],
    "addresses": [
        bigquery.SchemaField("_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_neighborhood", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_user", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("invoiceType", "STRING", mode="NULLABLE"), 
    ],
    "countries": [
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    ],
    "states": [
        bigquery.SchemaField("_country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    ],
    "cities": [
        bigquery.SchemaField("_country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    ],
    "neighborhoods": [
        bigquery.SchemaField("_city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("postalCode", "STRING", mode="NULLABLE"),
    ],
}


def process_and_load_csv(file_name, table_name, partition_field):
    """
    Processes partitioned files (users, orders, subscriptions, shipments) to 
    format date columns and enforces column order before saving and loading.
    """
    
    file_path = os.path.join(RAW_DATA_PATH, file_name)
    df = pd.read_csv(file_path)
    
    # 1. Clean Column Headers to Match Schema (Minimal Prep)
    df.columns = [clean_col_name(c) for c in df.columns]

    # 2. Shipments Specific Pre-Processing
    if table_name == 'shipments' and partition_field == 'collectDate':
        df['collectDate'] = extract_shipment_date(df['details'])
    
    # --- Prepare Data Types for BigQuery (MANDATORY for TIMESTAMP fields) ---
    if partition_field in df.columns:
        df[partition_field] = pd.to_datetime(df[partition_field], errors='coerce', utc=True)
        # Convert to string format that BigQuery reliably ingests as TIMESTAMP
        df[partition_field] = df[partition_field].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    # 3. CRITICAL FIX: Reorder DataFrame columns to EXACTLY match the schema order
    schema_field_names = [field.name for field in PARTITIONED_SCHEMAS.get(table_name)]
    df = df[schema_field_names]

    # 4. Save Cleaned DataFrame to a Local Temporary File
    df.to_csv(TEMP_FILE_PATH, index=False)
    
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(table_name)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        skip_leading_rows=1, 
        autodetect=False, 
        schema=PARTITIONED_SCHEMAS.get(table_name) 
    )
    
    print(f"Loading processed table {table_name}, partitioned by {partition_field}...")

    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field, 
    )
    
    # 5. Load data from the local file
    try:
        with open(TEMP_FILE_PATH, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Successfully loaded {table_name} with {len(df)} rows.")
    finally:
        if os.path.exists(TEMP_FILE_PATH):
            os.remove(TEMP_FILE_PATH)

def load_csv_directly(file_name, table_name):
    """
    Loads non-partitioned files (reference data) directly from the raw CSV path 
    using the explicit schema.
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(table_name)
    file_path = os.path.join(RAW_DATA_PATH, file_name)

    # We use the explicit schema for type checking even on direct loads
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        autodetect=False,
        skip_leading_rows=1,
        schema=PARTITIONED_SCHEMAS.get(table_name)
    )

    print(f"Loading direct reference table {table_name}...")
    
    try:
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Successfully loaded {table_name} directly.")
    except Exception as e:
        print(f"❌ Failed direct load for {table_name}. Error: {e}")
        raise

def main():
    # Ensure the raw_data dataset exists (Idempotence)
    client = bigquery.Client(project=PROJECT_ID)
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset.location = "US" 
    client.create_dataset(dataset, exists_ok=True)

    # ------------------ Files Needing Processing (Date Formatting and Reordering) ------------------
    processed_configs = [
        {"file": "users (1).csv", "table": "users", "partition": "createdAt"}, 
        {"file": "orders (1).csv", "table": "orders", "partition": "createdAt"},
        {"file": "subscriptions (1).csv", "table": "subscriptions", "partition": "createdAt"},
        {"file": "shipments (1).csv", "table": "shipments", "partition": "collectDate"}, 
    ]
    
    for config in processed_configs:
        try:
            process_and_load_csv(config["file"], config["table"], config["partition"])
        except Exception as e:
            print(f"❌ Failed to process and load {config['file']}. Error: {e}")
            raise

    # ------------------ Files Loaded Directly (Reference Data) ------------------
    direct_configs = [
        {"file": "Marketing Spend (TRY).csv", "table": "marketing_spend"}, 
        {"file": "addresses (1).csv", "table": "addresses"},
        {"file": "countries (1).csv", "table": "countries"},
        {"file": "states (1).csv", "table": "states"},
        {"file": "cities (1).csv", "table": "cities"},
        {"file": "neighborhoods (1).csv", "table": "neighborhoods"},
    ]

    for config in direct_configs:
        try:
            load_csv_directly(config["file"], config["table"])
        except Exception as e:
            print(f"❌ Failed to load directly {config['file']}. Error: {e}")
            raise

if __name__ == "__main__":
    main()