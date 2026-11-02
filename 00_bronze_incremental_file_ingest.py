# Databricks notebook source
# ForgeAxis: file-driven Bronze ingestion for ALL seven datasets.
# Import as one Python notebook on Serverless.
# Upload CSV files to the incoming folders; this notebook never generates test data.
# COPY INTO remembers loaded file paths and skips them on later runs.
# Existing Silver and Gold notebooks do not need to be rewritten.

# COMMAND ----------
# 1. Configuration: one stable folder per dataset.
from uuid import uuid4

VOLUME = "/Volumes/forgeaxis/forgeaxis_bronze/fg_bronze_data"
CATALOG_SCHEMA = "forgeaxis.forgeaxis_bronze"
BATCH_ID = "bronze_files_" + str(uuid4())

DATASETS = [
    ("customers", "bronze_customer", ["customer_id","customer_name","city","state","customer_type","credit_limit","status","created_at"]),
    ("products", "bronze_products", ["product_id","product_name","categories","unit_price","manufacturing_cost","status","created_at"]),
    ("machines", "bronze_machine_master", ["machine_id","machine_name","production_line","capacity_per_hour"]),
    ("sales_orders", "bronze_sales_orders", ["sales_order_id","customer_id","product_id","quantity_ordered","unit_price","total_amount","order_date","required_date","order_status"]),
    ("production_orders", "bronze_production_orders", ["production_order_id","sales_order_id","machine_id","quantity_produced","start_time","end_time","production_status"]),
    ("quality_checks", "bronze_quality_checks", ["quality_check_id","production_order_id","inspector_name","inspection_time","defect_type","defect_count","quality_status","remarks"]),
    ("sensor_logs", "bronze_sensor_logs", ["sensor_log_id","production_order_id","machine_id","timestamp","temperature","pressure","vibration","humidity"]),
]

# COMMAND ----------
# 2. Create the incoming folders and check tables that have CSV files.
# The original CSVs and practice files stay outside these folders.
# Add the three lineage columns if a full-load table does not yet have them.
METADATA_COLUMNS = {
    "source_file": "STRING",
    "bronze_loaded_at": "TIMESTAMP",
    "batch_id": "STRING",
}

def has_csv(folder):
    return any(file.path.lower().endswith(".csv") for file in dbutils.fs.ls(folder))

for folder, table_name, business_columns in DATASETS:
    incoming_folder = f"{VOLUME}/incoming/{folder}"
    dbutils.fs.mkdirs(incoming_folder)
    print(folder, "upload folder:", incoming_folder)

    if not has_csv(incoming_folder):
        continue

    table = f"{CATALOG_SCHEMA}.{table_name}"
    existing_columns = spark.table(table).columns
    missing_business = [name for name in business_columns if name not in existing_columns]
    if missing_business:
        raise ValueError(f"{table} is missing business columns: {missing_business}")

    for name, data_type in METADATA_COLUMNS.items():
        if name not in existing_columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({name} {data_type})")

# COMMAND ----------
# 3. COPY INTO: one statement per source dataset.
# Each command scans its folder and skips files it already loaded.
# Do not set FORCE = true; that would reload old files.

# COMMAND ----------
# customers: bronze_customer
folder = f"{VOLUME}/incoming/customers"
if has_csv(folder):
    display(spark.sql(f"""
COPY INTO {CATALOG_SCHEMA}.bronze_customer
FROM (
    SELECT
        customer_id,
        customer_name,
        city,
        state,
        customer_type,
        try_cast(try_cast(credit_limit AS DECIMAL(38, 2)) AS INT) AS credit_limit,
        status,
        CAST(coalesce(try_cast(created_at AS TIMESTAMP), try_to_timestamp(created_at, 'dd-MM-yyyy HH:mm:ss'), try_to_timestamp(created_at, 'dd-MM-yyyy')) AS DATE) AS created_at,
        _metadata.file_path AS source_file,
        current_timestamp() AS bronze_loaded_at,
        '{BATCH_ID}' AS batch_id
    FROM '{folder}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
"""))
else:
    print("customers: no CSV files yet")

# COMMAND ----------
# products: bronze_products
folder = f"{VOLUME}/incoming/products"
if has_csv(folder):
    display(spark.sql(f"""
COPY INTO {CATALOG_SCHEMA}.bronze_products
FROM (
    SELECT
        product_id,
        product_name,
        categories,
        try_cast(unit_price AS DOUBLE) AS unit_price,
        try_cast(manufacturing_cost AS DOUBLE) AS manufacturing_cost,
        status,
        coalesce(try_cast(created_at AS TIMESTAMP), try_to_timestamp(created_at, 'dd-MM-yyyy HH:mm:ss'), try_to_timestamp(created_at, 'dd-MM-yyyy')) AS created_at,
        _metadata.file_path AS source_file,
        current_timestamp() AS bronze_loaded_at,
        '{BATCH_ID}' AS batch_id
    FROM '{folder}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
"""))
else:
    print("products: no CSV files yet")

# COMMAND ----------
# machines: bronze_machine_master
folder = f"{VOLUME}/incoming/machines"
if has_csv(folder):
    display(spark.sql(f"""
COPY INTO {CATALOG_SCHEMA}.bronze_machine_master
FROM (
    SELECT
        machine_id,
        machine_name,
        production_line,
        try_cast(try_cast(capacity_per_hour AS DECIMAL(38, 2)) AS INT) AS capacity_per_hour,
        _metadata.file_path AS source_file,
        current_timestamp() AS bronze_loaded_at,
        '{BATCH_ID}' AS batch_id
    FROM '{folder}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
"""))
else:
    print("machines: no CSV files yet")

# COMMAND ----------
# sales_orders: bronze_sales_orders
folder = f"{VOLUME}/incoming/sales_orders"
if has_csv(folder):
    display(spark.sql(f"""
COPY INTO {CATALOG_SCHEMA}.bronze_sales_orders
FROM (
    SELECT
        sales_order_id,
        customer_id,
        product_id,
        try_cast(try_cast(quantity_ordered AS DECIMAL(38, 2)) AS INT) AS quantity_ordered,
        try_cast(unit_price AS DOUBLE) AS unit_price,
        try_cast(total_amount AS DOUBLE) AS total_amount,
        CAST(coalesce(try_cast(order_date AS TIMESTAMP), try_to_timestamp(order_date, 'dd-MM-yyyy')) AS DATE) AS order_date,
        CAST(coalesce(try_cast(required_date AS TIMESTAMP), try_to_timestamp(required_date, 'dd-MM-yyyy')) AS DATE) AS required_date,
        order_status,
        _metadata.file_path AS source_file,
        current_timestamp() AS bronze_loaded_at,
        '{BATCH_ID}' AS batch_id
    FROM '{folder}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
"""))
else:
    print("sales_orders: no CSV files yet")

# COMMAND ----------
# production_orders: bronze_production_orders
folder = f"{VOLUME}/incoming/production_orders"
if has_csv(folder):
    display(spark.sql(f"""
COPY INTO {CATALOG_SCHEMA}.bronze_production_orders
FROM (
    SELECT
        production_order_id,
        sales_order_id,
        machine_id,
        try_cast(try_cast(quantity_produced AS DECIMAL(38, 2)) AS INT) AS quantity_produced,
        coalesce(try_cast(start_time AS TIMESTAMP), try_to_timestamp(start_time, 'dd-MM-yyyy HH:mm:ss'), try_to_timestamp(start_time, 'dd-MM-yyyy HH:mm')) AS start_time,
        coalesce(try_cast(end_time AS TIMESTAMP), try_to_timestamp(end_time, 'dd-MM-yyyy HH:mm:ss'), try_to_timestamp(end_time, 'dd-MM-yyyy HH:mm')) AS end_time,
        production_status,
        _metadata.file_path AS source_file,
        current_timestamp() AS bronze_loaded_at,
        '{BATCH_ID}' AS batch_id
    FROM '{folder}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
"""))
else:
    print("production_orders: no CSV files yet")

# COMMAND ----------
# quality_checks: bronze_quality_checks
folder = f"{VOLUME}/incoming/quality_checks"
if has_csv(folder):
    display(spark.sql(f"""
COPY INTO {CATALOG_SCHEMA}.bronze_quality_checks
FROM (
    SELECT
        quality_check_id,
        production_order_id,
        inspector_name,
        coalesce(try_cast(inspection_time AS TIMESTAMP), try_to_timestamp(inspection_time, 'dd-MM-yyyy HH:mm:ss'), try_to_timestamp(inspection_time, 'dd-MM-yyyy HH:mm')) AS inspection_time,
        defect_type,
        try_cast(try_cast(defect_count AS DECIMAL(38, 2)) AS INT) AS defect_count,
        quality_status,
        remarks,
        _metadata.file_path AS source_file,
        current_timestamp() AS bronze_loaded_at,
        '{BATCH_ID}' AS batch_id
    FROM '{folder}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
"""))
else:
    print("quality_checks: no CSV files yet")

# COMMAND ----------
# sensor_logs: bronze_sensor_logs
folder = f"{VOLUME}/incoming/sensor_logs"
if has_csv(folder):
    display(spark.sql(f"""
COPY INTO {CATALOG_SCHEMA}.bronze_sensor_logs
FROM (
    SELECT
        sensor_log_id,
        production_order_id,
        machine_id,
        coalesce(try_cast(`timestamp` AS TIMESTAMP), try_to_timestamp(`timestamp`, 'dd-MM-yyyy HH:mm:ss'), try_to_timestamp(`timestamp`, 'dd-MM-yyyy HH:mm')) AS `timestamp`,
        try_cast(temperature AS DOUBLE) AS temperature,
        try_cast(pressure AS DOUBLE) AS pressure,
        try_cast(vibration AS DOUBLE) AS vibration,
        try_cast(humidity AS DOUBLE) AS humidity,
        _metadata.file_path AS source_file,
        current_timestamp() AS bronze_loaded_at,
        '{BATCH_ID}' AS batch_id
    FROM '{folder}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
"""))
else:
    print("sensor_logs: no CSV files yet")

# COMMAND ----------
# 4. Verify one new batch across all seven Bronze tables.
# No rows here is normal when this run found no new files.
for folder, table_name, _ in DATASETS:
    table = f"{CATALOG_SCHEMA}.{table_name}"
    rows = spark.table(table).filter(f"batch_id = '{BATCH_ID}'").count()
    print(f"{table}: {rows} rows in this run")
print("Bronze file ingestion finished. Batch ID:", BATCH_ID)

# NEXT: run the existing Silver incremental notebooks in parent-first order,
# then the existing Gold dimension and fact notebooks.

