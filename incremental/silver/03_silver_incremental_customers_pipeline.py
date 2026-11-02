# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: Customers Silver Incremental Pipeline
# Start after the completed Bronze incremental append.
# Import into Databricks as a Python notebook and run cells in order.

# COMMAND ----------

# Customers: Continue from Bronze to Silver Incremental
# Bronze append is complete. Run these new Silver cells in order.
# If any processing cell fails, stop before advancing the watermark.

# COMMAND ----------

# 1. Set the names and imports
# Create a new Python notebook named `03_Silver_Incremental_Customers_Step_By_Step` using Serverless. Use the plural Bronze name from your completed notebook.

from datetime import datetime
from pyspark.sql.functions import (
    col, lit, trim, lower, when, expr, row_number
)
from pyspark.sql.window import Window

BRONZE = "forgeaxis.forgeaxis_bronze.bronze_customer"
SILVER = "forgeaxis.forgeaxis_silver.silver_customers"
WATERMARK = "forgeaxis.forgeaxis_silver.watermark"
AUDIT = "forgeaxis.forgeaxis_silver.pipeline_audit"
ERRORS = "forgeaxis.forgeaxis_silver.silver_error_records"

PIPELINE = "customers_bronze_to_silver"
start_time = datetime.now()
batch_id = start_time.strftime("%Y%m%d_%H%M%S_%f")

print("Silver attempt:", batch_id)

# COMMAND ----------

# 2. Create the watermark table
# `pipeline_name` identifies Customers. The timestamp is the checkpoint. `CREATE TABLE IF NOT EXISTS` keeps existing checkpoints.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK} (
    pipeline_name STRING,
    last_processed_timestamp TIMESTAMP,
    last_batch_id STRING,
    updated_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# 3. Add the initial Customers checkpoint
# Insert the starting checkpoint only when the row is absent. Do not overwrite an existing checkpoint. Starting at 1900 processes timestamped Bronze arrivals once; the Silver MERGE handles existing IDs.

spark.sql(f"""
MERGE INTO {WATERMARK} AS target
USING (
    SELECT '{PIPELINE}' AS pipeline_name
) AS source
ON target.pipeline_name = source.pipeline_name

WHEN NOT MATCHED THEN INSERT (
    pipeline_name,
    last_processed_timestamp,
    last_batch_id,
    updated_at
)
VALUES (
    source.pipeline_name,
    TIMESTAMP '1900-01-01 00:00:00',
    NULL,
    current_timestamp()
)
""")

display(spark.table(WATERMARK))

# COMMAND ----------

# 4. Read the saved watermark
# `collect()` brings this one control row into Python. We expect exactly one Customers checkpoint.

checkpoint_rows = (
    spark.table(WATERMARK)
    .filter(col("pipeline_name") == PIPELINE)
    .collect()
)

assert len(checkpoint_rows) == 1, "Expected one Customers checkpoint."

last_watermark = checkpoint_rows[0]["last_processed_timestamp"]

print("Previous watermark:", last_watermark)

# COMMAND ----------

# 5. Read one fixed Bronze snapshot
# Delta stores table versions. Reading one version means every following step sees the same committed input. Finish Bronze ingestion before this lesson; run one Customers pipeline at a time.

bronze_version = spark.sql(
    f"DESCRIBE HISTORY {BRONZE} LIMIT 1"
).first()["version"]

bronze_df = (
    spark.read
    .option("versionAsOf", bronze_version)
    .table(BRONZE)
)

assert dict(bronze_df.dtypes).get("bronze_loaded_at") == "timestamp", (
    "Expected bronze_loaded_at TIMESTAMP."
)

# COMMAND ----------

# 6. Select new Bronze arrivals and customers still missing from Silver.
# The second group repairs a missed customer even when its arrival time is
# older than the saved watermark. Both groups follow the same quality checks.

upper_watermark = bronze_df.selectExpr(
    "max(bronze_loaded_at) AS newest_arrival"
).first()["newest_arrival"]

new_arrivals_df = bronze_df.filter(
    (col("bronze_loaded_at") > lit(last_watermark)) &
    (col("bronze_loaded_at") <= lit(upper_watermark))
)

silver_customer_ids = spark.table(SILVER).select("customer_id").distinct()
missing_from_silver_df = (
    bronze_df
    .filter(col("bronze_loaded_at").isNotNull())
    .filter(col("bronze_loaded_at") <= lit(last_watermark))
    .join(silver_customer_ids, "customer_id", "left_anti")
)

new_arrival_count = new_arrivals_df.count()
backfill_count = missing_from_silver_df.count()
incremental_df = new_arrivals_df.unionByName(missing_from_silver_df)
if new_arrival_count + backfill_count == 0:
    dbutils.notebook.exit("NO_DATA: no new arrivals or missing Silver customers.")

source_count = new_arrival_count + backfill_count
silver_before = spark.table(SILVER).count()

print("New Bronze arrivals:", new_arrival_count)
print("Older Bronze rows missing from Silver:", backfill_count)
print("Silver rows before:", silver_before)
print("Upper boundary:", upper_watermark)

display(incremental_df)

# COMMAND ----------

# 7. Clean the incremental records
# Use the same cleaning ideas as full load. Convert credit limit to DOUBLE and created_at to TIMESTAMP to match the full-load Silver design. `try_cast` returns null for invalid text so the next step can reject it.

clean_df = (
    incremental_df
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("customer_type", trim(col("customer_type")))
    .withColumn("status", lower(trim(col("status"))))
    .withColumn("_credit_raw", col("credit_limit"))
    .withColumn("_created_raw", col("created_at"))
    .withColumn("credit_limit", expr("try_cast(credit_limit AS DOUBLE)"))
    .withColumn("created_at", expr("try_cast(created_at AS TIMESTAMP)"))
)

display(clean_df)

# COMMAND ----------

# 8. Mark invalid rows
# A valid row has no error reason. `when` checks each condition in order and records the first applicable reason. A null credit limit remains allowed, matching your full-load rule.

checked_df = clean_df.withColumn(
    "error_reason",
    when(
        col("customer_id").isNull() | (col("customer_id") == ""),
        "Missing customer_id"
    )
    .when(
        col("customer_name").isNull() | (col("customer_name") == ""),
        "Missing customer_name"
    )
    .when(
        col("_credit_raw").isNotNull() & col("credit_limit").isNull(),
        "Invalid credit_limit format"
    )
    .when(col("credit_limit") < 0, "Negative credit_limit")
    .when(
        col("_created_raw").isNotNull() & col("created_at").isNull(),
        "Invalid created_at format"
    )
)

display(checked_df.select(
    "customer_id", "credit_limit", "error_reason"
))

# COMMAND ----------

# 9. Separate valid and rejected records
# The two filters separate the paths. Invalid rows will not be submitted to the Silver MERGE.

rejected_df = checked_df.filter(col("error_reason").isNotNull())

valid_df = (
    checked_df
    .filter(col("error_reason").isNull())
    .drop("error_reason", "_credit_raw", "_created_raw")
    .dropDuplicates()
)

rejected_count = rejected_df.count()

print("Rejected rows:", rejected_count)
display(rejected_df)

# COMMAND ----------

# 10. Keep the latest valid version per customer
# Several arriving batches can contain the same customer. Order by arrival time, then keep row 1. Two conflicting versions with the same arrival time are ambiguous: stop rather than choose randomly.

conflicts_df = (
    valid_df
    .groupBy("customer_id", "bronze_loaded_at")
    .count()
    .filter(col("count") > 1)
)

assert conflicts_df.limit(1).count() == 0, (
    "Conflicting versions have the same arrival time. Inspect Bronze first."
)

latest_first = Window.partitionBy("customer_id").orderBy(
    col("bronze_loaded_at").desc()
)

merge_df = (
    valid_df
    .withColumn("row_number", row_number().over(latest_first))
    .filter(col("row_number") == 1)
    .drop("row_number")
    .withColumn("batch_id", lit(batch_id))
    .withColumn("silver_loaded_at", lit(start_time))
)

silver_count = merge_df.count()
versions_removed = source_count - rejected_count - silver_count

print("Customers to MERGE:", silver_count)
print("Duplicate/older valid versions removed:", versions_removed)

display(merge_df)

# COMMAND ----------

# 11. Save rejection references
# This uses the existing error-table layout. Store the ID and reason; the full source record stays in Bronze. If CINC004 arrives with credit_limit = -100, its reason is Negative credit_limit.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERRORS} (
    batch_id STRING,
    table_name STRING,
    record_id STRING,
    error_reason STRING,
    error_time TIMESTAMP
)
USING DELTA
""")

error_df = rejected_df.select(
    lit(batch_id).alias("batch_id"),
    lit("silver_customers").alias("table_name"),
    col("customer_id").alias("record_id"),
    "error_reason",
    lit(start_time).alias("error_time")
)

if rejected_count > 0:
    error_df.write.format("delta").mode("append").saveAsTable(ERRORS)

print("Rejected records saved:", rejected_count)

# COMMAND ----------

# 12. Prepare the Silver MERGE
# Silver needs one record per customer ID. A temporary view makes the cleaned DataFrame available to SQL in this notebook session.

duplicate_targets = (
    spark.table(SILVER)
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
)

assert duplicate_targets.limit(1).count() == 0, (
    "Silver has duplicate customer IDs. Resolve them before MERGE."
)

merge_df.createOrReplaceTempView("customers_incremental_clean")

# COMMAND ----------

# 13. Update existing customers and insert new customers
# Match on customer_id. MATCHED updates the existing record; NOT MATCHED inserts a new record. This lesson explicitly loads the eight business fields and two Silver audit fields. It does not overwrite the table.

spark.sql(f"""
MERGE INTO {SILVER} AS target
USING customers_incremental_clean AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN UPDATE SET
    target.customer_name = source.customer_name,
    target.city = source.city,
    target.state = source.state,
    target.customer_type = source.customer_type,
    target.credit_limit = source.credit_limit,
    target.status = source.status,
    target.created_at = source.created_at,
    target.batch_id = source.batch_id,
    target.silver_loaded_at = source.silver_loaded_at

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_name,
    city,
    state,
    customer_type,
    credit_limit,
    status,
    created_at,
    batch_id,
    silver_loaded_at
)
VALUES (
    source.customer_id,
    source.customer_name,
    source.city,
    source.state,
    source.customer_type,
    source.credit_limit,
    source.status,
    source.created_at,
    source.batch_id,
    source.silver_loaded_at
)
""")

print("Silver MERGE completed.")

# COMMAND ----------

# 14. Check the updated Silver records
# In the first successful practice run, C0001 and C0002 should be Pune/inactive, and the three new customers should be Mumbai/active. Compared with the pre-run count, Silver should gain three rows.

silver_after = spark.table(SILVER).count()

print("Silver before:", silver_before)
print("Silver after:", silver_after)
print("Increase:", silver_after - silver_before)

display(
    spark.table(SILVER)
    .filter(col("customer_id").isin("C0001", "C0002", "CINC001", "CINC002", "CINC003"))
    .select("customer_id", "customer_name", "city", "status",
            "credit_limit", "created_at", "batch_id", "silver_loaded_at")
    .orderBy("customer_id")
)

# COMMAND ----------

# 15. Record the successful processing attempt
# This uses your existing pipeline_audit layout. silver_count is the number of customer versions submitted to MERGE, not the total Silver count and not just new inserts.

spark.sql(f"""
INSERT INTO {AUDIT} (
    batch_id,
    table_name,
    start_time,
    end_time,
    source_count,
    silver_count,
    rejected_count,
    status,
    error_message
)
VALUES (
    '{batch_id}',
    'silver_customers',
    TIMESTAMP '{start_time}',
    current_timestamp(),
    {source_count},
    {silver_count},
    {rejected_count},
    'SUCCESS',
    NULL
)
""")

# COMMAND ----------

# 16. Advance the watermark last
# Run this only after all previous processing and audit cells succeed. Save the captured upper boundary, not the current clock. If any earlier cell fails, keep the old checkpoint.

# A backfill must never move the checkpoint backwards.
next_watermark = max(last_watermark, upper_watermark)
spark.createDataFrame(
    [(PIPELINE, next_watermark, batch_id, start_time)],
    """pipeline_name STRING,
       last_processed_timestamp TIMESTAMP,
       last_batch_id STRING,
       updated_at TIMESTAMP"""
).createOrReplaceTempView("customers_new_watermark")

spark.sql(f"""
MERGE INTO {WATERMARK} AS target
USING customers_new_watermark AS source
ON target.pipeline_name = source.pipeline_name

WHEN MATCHED THEN UPDATE SET
    target.last_processed_timestamp = source.last_processed_timestamp,
    target.last_batch_id = source.last_batch_id,
    target.updated_at = source.updated_at
""")

display(
    spark.table(WATERMARK)
    .filter(col("pipeline_name") == PIPELINE)
)

# COMMAND ----------

# 17. View audit results
# Check this attempt's batch ID.

display(
    spark.table(AUDIT)
    .filter(col("batch_id") == batch_id)
)

display(
    spark.table(ERRORS)
    .filter(col("batch_id") == batch_id)
)