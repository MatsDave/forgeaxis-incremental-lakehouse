# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: incremental Silver -> dim_product
# Existing Gold full-load table with IDENTITY product_sk is required.
# Run one writer at a time. Finish Silver and parent dimensions first.
# Dimensions use SCD Type 1: overwrite attributes, preserve the existing SK.
# No CREATE OR REPLACE, INSERT OVERWRITE or deletes are used here.

# COMMAND ----------

# 1. Configuration
from pyspark.sql.functions import col, lit, max as spark_max
from uuid import uuid4

SILVER = "forgeaxis.forgeaxis_silver.silver_products"
GOLD = "forgeaxis.forgeaxis_gold.dim_product"
WATERMARK = "forgeaxis.forgeaxis_silver.watermark"
PIPELINE = "dim_product_silver_to_gold"
KEY = "product_id"

# New ID generated on every notebook run, not a hardcoded batch value.
batch_id = "gold_incremental_" + str(uuid4())
run_time = spark.sql("SELECT current_timestamp() AS run_time").first()["run_time"]

# COMMAND ----------

# 2. Reuse the watermark table. Gold gets its OWN pipeline checkpoint.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK} (
    pipeline_name STRING,
    last_processed_timestamp TIMESTAMP,
    last_batch_id STRING,
    updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
MERGE INTO {WATERMARK} t
USING (SELECT '{PIPELINE}' AS pipeline_name) s
ON t.pipeline_name = s.pipeline_name
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, last_processed_timestamp, last_batch_id, updated_at
) VALUES (
    s.pipeline_name, TIMESTAMP '1900-01-01 00:00:00', NULL, current_timestamp()
)
""")

# COMMAND ----------

# 3. Read the checkpoint and freeze a Silver snapshot.
checkpoint_rows = (
    spark.table(WATERMARK).filter(col("pipeline_name") == PIPELINE).collect()
)
if len(checkpoint_rows) != 1:
    raise ValueError("Expected exactly one watermark row for this pipeline.")
previous_watermark = checkpoint_rows[0]["last_processed_timestamp"]

silver_version = spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 1").first()["version"]
silver_df = spark.read.option("versionAsOf", silver_version).table(SILVER)

if dict(silver_df.dtypes).get("silver_loaded_at") != "timestamp":
    raise ValueError("Silver must have silver_loaded_at of type TIMESTAMP.")
if silver_df.filter(col("silver_loaded_at").isNull()).limit(1).count() > 0:
    raise ValueError("Silver contains null silver_loaded_at values. Fix these before Gold.")

upper_watermark = silver_df.agg(spark_max("silver_loaded_at").alias("latest")).first()["latest"]
print("Previous Gold watermark:", previous_watermark)
print("Latest Silver arrival:", upper_watermark)

if upper_watermark is None or upper_watermark <= previous_watermark:
    dbutils.notebook.exit("No new Silver rows for this Gold table.")

# COMMAND ----------

# 4. Select only new or updated Silver rows.
# First run starts at 1900 and safely reconciles all current Silver rows.
changed_df = silver_df.filter(
    (col("silver_loaded_at") > lit(previous_watermark))
    & (col("silver_loaded_at") <= lit(upper_watermark))
)

source_count = changed_df.count()
if changed_df.filter(col(KEY).isNull()).limit(1).count() > 0:
    raise ValueError("Source business key is null.")
if changed_df.groupBy(KEY).count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("Silver contains duplicate business keys. Fix Silver before MERGE.")

gold_df = spark.table(GOLD)
required_gold_columns = ["product_id","product_name","categories","unit_price","manufacturing_cost","status","created_at","product_sk","batch_id","gold_loaded_at"]
missing = [name for name in required_gold_columns if name not in gold_df.columns]
if missing:
    raise ValueError(f"Gold table is missing columns: {missing}")
if gold_df.filter(col(KEY).isNull()).limit(1).count() > 0:
    raise ValueError("Gold contains a null business key.")
if gold_df.groupBy(KEY).count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("Gold contains duplicate business keys. Fix Gold before MERGE.")

changed_df.createOrReplaceTempView("gold_changed_silver")

# COMMAND ----------

# 5. Check dimension lookups, then prepare the Gold source.
# Production references remain business IDs; no fact-to-fact joins.

gold_source_df = spark.sql("""
SELECT
    s.product_id, s.product_name, s.categories, s.unit_price, s.manufacturing_cost, s.status, s.created_at
FROM gold_changed_silver s

""")

if gold_source_df.count() != source_count:
    raise ValueError("Lookup joins changed the row count. Check dimension uniqueness.")

gold_source_df = (
    gold_source_df
    .withColumn("batch_id", lit(batch_id))
    .withColumn("gold_loaded_at", lit(run_time).cast("timestamp"))
)
gold_source_df.createOrReplaceTempView("gold_incremental_source")
display(gold_source_df.limit(10))

# COMMAND ----------

# 6. MERGE using the business key.
# Existing row: update changed attributes, NEVER replace its identity SK.
# New row: omit product_sk; Databricks generates that identity value.
# Null-safe <=> also compares nullable attributes correctly.
# Unchanged rows keep their previous batch_id and gold_loaded_at.
merge_result = spark.sql(f"""
MERGE INTO {GOLD} t
USING gold_incremental_source s
ON t.product_id = s.product_id
WHEN MATCHED AND NOT (
    t.product_name <=> s.product_name
    AND t.categories <=> s.categories
    AND t.unit_price <=> s.unit_price
    AND t.manufacturing_cost <=> s.manufacturing_cost
    AND t.status <=> s.status
    AND t.created_at <=> s.created_at
) THEN UPDATE SET
    t.product_name = s.product_name,
    t.categories = s.categories,
    t.unit_price = s.unit_price,
    t.manufacturing_cost = s.manufacturing_cost,
    t.status = s.status,
    t.created_at = s.created_at,
    t.batch_id = s.batch_id,
    t.gold_loaded_at = s.gold_loaded_at
WHEN NOT MATCHED THEN INSERT (
    product_id, product_name, categories, unit_price, manufacturing_cost, status, created_at, batch_id, gold_loaded_at
) VALUES (
    s.product_id, s.product_name, s.categories, s.unit_price, s.manufacturing_cost, s.status, s.created_at, s.batch_id, s.gold_loaded_at
)
""")
display(merge_result)

# COMMAND ----------

# 7. Move the watermark ONLY after the Gold MERGE succeeds.
# Run top-to-bottom: do not run this cell if a previous cell failed.
# If MERGE fails the old watermark remains, allowing a rerun.
# MERGE and the watermark are separate transactions; replay is safe by key.
checkpoint_df = spark.createDataFrame(
    [(PIPELINE, upper_watermark, batch_id, run_time)],
    "pipeline_name string, last_processed_timestamp timestamp, "
    "last_batch_id string, updated_at timestamp"
)
checkpoint_df.createOrReplaceTempView("gold_checkpoint_update")

spark.sql(f"""
MERGE INTO {WATERMARK} t
USING gold_checkpoint_update s
ON t.pipeline_name = s.pipeline_name
WHEN MATCHED THEN UPDATE SET
    t.last_processed_timestamp = s.last_processed_timestamp,
    t.last_batch_id = s.last_batch_id,
    t.updated_at = s.updated_at
""")

print("Gold incremental completed:", GOLD)
print("Silver rows considered:", source_count)
print("Batch ID:", batch_id)

# COMMAND ----------

# 8. View rows inserted or changed by this run, plus the checkpoint.
# This may show fewer rows than source_count when some rows were unchanged.
display(spark.table(GOLD).filter(col("batch_id") == batch_id))
display(spark.table(WATERMARK).filter(col("pipeline_name") == PIPELINE))
