# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: incremental sensor_logs CSV files -> Bronze Delta table.
# Upload CSV files to INPUT_FOLDER. This notebook does not generate test data.

# COMMAND ----------

# 1. Names for this dataset.
from uuid import uuid4

CATALOG_SCHEMA = "forgeaxis.forgeaxis_bronze"
BRONZE = "forgeaxis.forgeaxis_bronze.bronze_sensor_logs"
INPUT_FOLDER = "/Volumes/forgeaxis/forgeaxis_bronze/fg_bronze_data/incoming/sensor_logs"
BUSINESS_COLUMNS = ["sensor_log_id","production_order_id","machine_id","timestamp","temperature","pressure","vibration","humidity"]
BATCH_ID = "sensor_logs_" + str(uuid4())

# COMMAND ----------

# 2. Look for uploaded CSV files. An empty folder is a successful no-data run.
dbutils.fs.mkdirs(INPUT_FOLDER)
csv_files = [f.name for f in dbutils.fs.ls(INPUT_FOLDER) if f.name.lower().endswith(".csv")]
print("Upload folder:", INPUT_FOLDER)
print("CSV files:", csv_files)
if not csv_files:
    dbutils.notebook.exit("NO_FILES: nothing to load.")

# COMMAND ----------

# 3. Check the existing Bronze table and its columns.
existing_columns = spark.table(BRONZE).columns
missing = [name for name in BUSINESS_COLUMNS if name not in existing_columns]
if missing:
    raise ValueError(f"{BRONZE} is missing columns: {missing}")

for name, data_type in [("source_file", "STRING"),
                        ("bronze_loaded_at", "TIMESTAMP"),
                        ("batch_id", "STRING")]:
    if name not in existing_columns:
        spark.sql(f"ALTER TABLE {BRONZE} ADD COLUMNS ({name} {data_type})")

# COMMAND ----------

# 4. COPY INTO appends only CSV files this table has not loaded before.
result = spark.sql(f"""
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
    FROM '{INPUT_FOLDER}'
)
FILEFORMAT = CSV
PATTERN = '*.csv'
FORMAT_OPTIONS ('header' = 'true', 'mode' = 'FAILFAST')
""")
display(result)

# COMMAND ----------

# 5. Check this run. A count of zero means the files were already loaded.
loaded = spark.table(BRONZE).filter(f"batch_id = '{BATCH_ID}'")
print("sensor_logs rows loaded:", loaded.count())
display(loaded.limit(10))
