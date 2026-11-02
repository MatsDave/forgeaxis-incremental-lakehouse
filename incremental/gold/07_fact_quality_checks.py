# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: incremental Silver -> fact_quality_checks
# Existing Gold full-load table with IDENTITY quality_check_sk is required.
# Run one writer at a time. Finish Silver and parent dimensions first.
# Dimensions use SCD Type 1: overwrite attributes, preserve the existing SK.
# No CREATE OR REPLACE, INSERT OVERWRITE or deletes are used here.

# COMMAND ----------

# 1. Configuration
from pyspark.sql.functions import col, lit, max as spark_max
from uuid import uuid4

SILVER = "forgeaxis.forgeaxis_silver.silver_quality_checks"
GOLD = "forgeaxis.forgeaxis_gold.fact_quality_checks"
WATERMARK = "forgeaxis.forgeaxis_silver.watermark"
PIPELINE = "fact_quality_checks_silver_to_gold"
KEY = "quality_check_id"

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
required_gold_columns = ["quality_check_id","production_order_id","inspector_name","inspection_date_key","inspection_time","defect_type","defect_count","quality_status","remarks","quality_check_sk","batch_id","gold_loaded_at"]
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

dimension_df = spark.table("forgeaxis.forgeaxis_gold.dim_date")
if dimension_df.filter(col("full_date").isNull()).limit(1).count() > 0:
    raise ValueError("dim_date contains a null full_date. Fix the dimension first.")
if dimension_df.groupBy("full_date").count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("dim_date has duplicate full_date values. Fix the dimension first.")

# A LEFT JOIN retains unmatched rows so we can stop instead of losing them.
missing_lookup_count = spark.sql("""
SELECT s.quality_check_id
FROM gold_changed_silver s
LEFT JOIN forgeaxis.forgeaxis_gold.dim_date d ON CAST(s.inspection_time AS DATE) = d.full_date
WHERE d.date_key IS NULL
""").count()
if missing_lookup_count > 0:
    raise ValueError(
        f"{missing_lookup_count} rows have missing dimension/date keys. "
        "Load dimensions first; extend dim_date if needed. Watermark is unchanged."
    )

gold_source_df = spark.sql("""
SELECT
    s.quality_check_id, s.production_order_id, s.inspector_name,
    d.date_key AS inspection_date_key, s.inspection_time,
    s.defect_type, s.defect_count, s.quality_status, s.remarks
FROM gold_changed_silver s
LEFT JOIN forgeaxis.forgeaxis_gold.dim_date d ON CAST(s.inspection_time AS DATE) = d.full_date
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
# New row: omit quality_check_sk; Databricks generates that identity value.
# Null-safe <=> also compares nullable attributes correctly.
# Unchanged rows keep their previous batch_id and gold_loaded_at.
merge_result = spark.sql(f"""
MERGE INTO {GOLD} t
USING gold_incremental_source s
ON t.quality_check_id = s.quality_check_id
WHEN MATCHED AND NOT (
    t.production_order_id <=> s.production_order_id
    AND t.inspector_name <=> s.inspector_name
    AND t.inspection_date_key <=> s.inspection_date_key
    AND t.inspection_time <=> s.inspection_time
    AND t.defect_type <=> s.defect_type
    AND t.defect_count <=> s.defect_count
    AND t.quality_status <=> s.quality_status
    AND t.remarks <=> s.remarks
) THEN UPDATE SET
    t.production_order_id = s.production_order_id,
    t.inspector_name = s.inspector_name,
    t.inspection_date_key = s.inspection_date_key,
    t.inspection_time = s.inspection_time,
    t.defect_type = s.defect_type,
    t.defect_count = s.defect_count,
    t.quality_status = s.quality_status,
    t.remarks = s.remarks,
    t.batch_id = s.batch_id,
    t.gold_loaded_at = s.gold_loaded_at
WHEN NOT MATCHED THEN INSERT (
    quality_check_id, production_order_id, inspector_name, inspection_date_key, inspection_time, defect_type, defect_count, quality_status, remarks, batch_id, gold_loaded_at
) VALUES (
    s.quality_check_id, s.production_order_id, s.inspector_name, s.inspection_date_key, s.inspection_time, s.defect_type, s.defect_count, s.quality_status, s.remarks, s.batch_id, s.gold_loaded_at
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
