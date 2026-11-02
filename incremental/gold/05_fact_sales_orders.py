# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: incremental Silver -> fact_sales_orders
# Existing Gold full-load table with IDENTITY sales_order_sk is required.
# Run one writer at a time. Finish Silver and parent dimensions first.
# Dimensions use SCD Type 1: overwrite attributes, preserve the existing SK.
# No CREATE OR REPLACE, INSERT OVERWRITE or deletes are used here.

# COMMAND ----------

# 1. Configuration
from pyspark.sql.functions import col, lit, max as spark_max
from uuid import uuid4

SILVER = "forgeaxis.forgeaxis_silver.silver_sales_orders"
GOLD = "forgeaxis.forgeaxis_gold.fact_sales_orders"
WATERMARK = "forgeaxis.forgeaxis_silver.watermark"
PIPELINE = "fact_sales_orders_silver_to_gold"
KEY = "sales_order_id"

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
required_gold_columns = ["sales_order_id","customer_sk","product_sk","quantity_ordered","unit_price","total_amount","order_date_key","required_date_key","order_status","sales_order_sk","batch_id","gold_loaded_at"]
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

dimension_df = spark.table("forgeaxis.forgeaxis_gold.dim_customer")
if dimension_df.filter(col("customer_id").isNull()).limit(1).count() > 0:
    raise ValueError("dim_customer contains a null customer_id. Fix the dimension first.")
if dimension_df.groupBy("customer_id").count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("dim_customer has duplicate customer_id values. Fix the dimension first.")

dimension_df = spark.table("forgeaxis.forgeaxis_gold.dim_product")
if dimension_df.filter(col("product_id").isNull()).limit(1).count() > 0:
    raise ValueError("dim_product contains a null product_id. Fix the dimension first.")
if dimension_df.groupBy("product_id").count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("dim_product has duplicate product_id values. Fix the dimension first.")

dimension_df = spark.table("forgeaxis.forgeaxis_gold.dim_date")
if dimension_df.filter(col("full_date").isNull()).limit(1).count() > 0:
    raise ValueError("dim_date contains a null full_date. Fix the dimension first.")
if dimension_df.groupBy("full_date").count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("dim_date has duplicate full_date values. Fix the dimension first.")

# A LEFT JOIN retains unmatched rows so we can stop instead of losing them.
missing_lookup_count = spark.sql("""
SELECT s.sales_order_id
FROM gold_changed_silver s
LEFT JOIN forgeaxis.forgeaxis_gold.dim_customer c ON s.customer_id = c.customer_id
LEFT JOIN forgeaxis.forgeaxis_gold.dim_product p ON s.product_id = p.product_id
LEFT JOIN forgeaxis.forgeaxis_gold.dim_date od ON CAST(s.order_date AS DATE) = od.full_date
LEFT JOIN forgeaxis.forgeaxis_gold.dim_date rd ON CAST(s.required_date AS DATE) = rd.full_date
WHERE c.customer_sk IS NULL OR p.product_sk IS NULL OR od.date_key IS NULL OR (s.required_date IS NOT NULL AND rd.date_key IS NULL)
""").count()
if missing_lookup_count > 0:
    raise ValueError(
        f"{missing_lookup_count} rows have missing dimension/date keys. "
        "Load dimensions first; extend dim_date if needed. Watermark is unchanged."
    )

gold_source_df = spark.sql("""
SELECT
    s.sales_order_id, c.customer_sk, p.product_sk,
    s.quantity_ordered, s.unit_price, s.total_amount,
    od.date_key AS order_date_key, rd.date_key AS required_date_key, s.order_status
FROM gold_changed_silver s
LEFT JOIN forgeaxis.forgeaxis_gold.dim_customer c ON s.customer_id = c.customer_id
LEFT JOIN forgeaxis.forgeaxis_gold.dim_product p ON s.product_id = p.product_id
LEFT JOIN forgeaxis.forgeaxis_gold.dim_date od ON CAST(s.order_date AS DATE) = od.full_date
LEFT JOIN forgeaxis.forgeaxis_gold.dim_date rd ON CAST(s.required_date AS DATE) = rd.full_date
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

# MAGIC %sql
# MAGIC select * from forgeaxis.forgeaxis_gold.dim_customer
# MAGIC where customer_id = 'C0291' and customer_id = 'C0447';

# COMMAND ----------

# 6. MERGE using the business key.
# Existing row: update changed attributes, NEVER replace its identity SK.
# New row: omit sales_order_sk; Databricks generates that identity value.
# Null-safe <=> also compares nullable attributes correctly.
# Unchanged rows keep their previous batch_id and gold_loaded_at.
merge_result = spark.sql(f"""
MERGE INTO {GOLD} t
USING gold_incremental_source s
ON t.sales_order_id = s.sales_order_id
WHEN MATCHED AND NOT (
    t.customer_sk <=> s.customer_sk
    AND t.product_sk <=> s.product_sk
    AND t.quantity_ordered <=> s.quantity_ordered
    AND t.unit_price <=> s.unit_price
    AND t.total_amount <=> s.total_amount
    AND t.order_date_key <=> s.order_date_key
    AND t.required_date_key <=> s.required_date_key
    AND t.order_status <=> s.order_status
) THEN UPDATE SET
    t.customer_sk = s.customer_sk,
    t.product_sk = s.product_sk,
    t.quantity_ordered = s.quantity_ordered,
    t.unit_price = s.unit_price,
    t.total_amount = s.total_amount,
    t.order_date_key = s.order_date_key,
    t.required_date_key = s.required_date_key,
    t.order_status = s.order_status,
    t.batch_id = s.batch_id,
    t.gold_loaded_at = s.gold_loaded_at
WHEN NOT MATCHED THEN INSERT (
    sales_order_id, customer_sk, product_sk, quantity_ordered, unit_price, total_amount, order_date_key, required_date_key, order_status, batch_id, gold_loaded_at
) VALUES (
    s.sales_order_id, s.customer_sk, s.product_sk, s.quantity_ordered, s.unit_price, s.total_amount, s.order_date_key, s.required_date_key, s.order_status, s.batch_id, s.gold_loaded_at
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
