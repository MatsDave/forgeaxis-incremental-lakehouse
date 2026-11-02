# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: incremental machines CSV files -> Bronze Delta table.
# Upload CSV files to INPUT_FOLDER. This notebook does not generate test data.

# COMMAND ----------

# 1. Names for this dataset.
from uuid import uuid4

CATALOG_SCHEMA = "forgeaxis.forgeaxis_bronze"
BRONZE = "forgeaxis.forgeaxis_bronze.bronze_machine"
INPUT_FOLDER = "/Volumes/forgeaxis/forgeaxis_bronze/fg_bronze_data/incoming/machines"
BUSINESS_COLUMNS = ["machine_id","machine_name","production_line","capacity_per_hour"]
BATCH_ID = "machines_" + str(uuid4())

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
COPY INTO {CATALOG_SCHEMA}.bronze_machine
FROM (
    SELECT
        machine_id,
        machine_name,
        production_line,
        try_cast(try_cast(capacity_per_hour AS DECIMAL(38, 2)) AS INT) AS capacity_per_hour,
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
print("machines rows loaded:", loaded.count())
display(loaded.limit(10))
