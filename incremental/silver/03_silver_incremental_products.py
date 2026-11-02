# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: products Silver incremental pipeline
# Import this .py file into Databricks. Run with Python + Serverless.
# Bronze incremental append must already be complete.
# Run only one writer per dataset; finish Bronze writes before Silver.
# Existing Silver full-load tables are required.
# New Bronze arrivals must have a fresh bronze_loaded_at TIMESTAMP.

# COMMAND ----------

# 1. Imports and configuration
from datetime import datetime
from uuid import uuid4
from pyspark.sql.functions import col, lit, trim, lower, when, expr, row_number
from pyspark.sql.window import Window

BRONZE = "forgeaxis.forgeaxis_bronze.bronze_products"
SILVER = "forgeaxis.forgeaxis_silver.silver_products"
WATERMARK = "forgeaxis.forgeaxis_silver.watermark"
AUDIT = "forgeaxis.forgeaxis_silver.pipeline_audit"
ERRORS = "forgeaxis.forgeaxis_silver.silver_error_records"

PIPELINE = "products_bronze_to_silver"
KEY = "product_id"
TABLE_NAME = "silver_products"
BUSINESS_COLUMNS = ["product_id","product_name","categories","unit_price","manufacturing_cost","status","created_at"]

start_time = datetime.now()
batch_id = str(uuid4())

# COMMAND ----------

# 2. Create/reuse the watermark and log tables.
# These CREATE statements do not replace existing tables or checkpoints.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {WATERMARK} (
    pipeline_name STRING,
    last_processed_timestamp TIMESTAMP,
    last_batch_id STRING,
    updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT} (
    batch_id STRING,
    table_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    source_count BIGINT,
    silver_count BIGINT,
    rejected_count BIGINT,
    status STRING,
    error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERRORS} (
    batch_id STRING,
    table_name STRING,
    record_id STRING,
    error_reason STRING,
    error_time TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
MERGE INTO {WATERMARK} AS target
USING (SELECT '{PIPELINE}' AS pipeline_name) AS source
ON target.pipeline_name = source.pipeline_name
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, last_processed_timestamp, last_batch_id, updated_at
) VALUES (
    source.pipeline_name, TIMESTAMP '1900-01-01 00:00:00',
    NULL, current_timestamp()
)
""")

# COMMAND ----------

# 3. Read the previous checkpoint and a fixed Bronze snapshot.
checkpoint_rows = (
    spark.table(WATERMARK)
    .filter(col("pipeline_name") == PIPELINE)
    .collect()
)
assert len(checkpoint_rows) == 1, "Expected one watermark row for this pipeline."
last_watermark = checkpoint_rows[0]["last_processed_timestamp"]

bronze_version = spark.sql(
    f"DESCRIBE HISTORY {BRONZE} LIMIT 1"
).first()["version"]

bronze_df = (
    spark.read.option("versionAsOf", bronze_version).table(BRONZE)
)
assert dict(bronze_df.dtypes).get("bronze_loaded_at") == "timestamp", (
    "Bronze needs bronze_loaded_at TIMESTAMP."
)

missing_source = [name for name in BUSINESS_COLUMNS if name not in bronze_df.columns]
assert not missing_source, f"Missing Bronze columns: {missing_source}"

missing_target = [
    name for name in BUSINESS_COLUMNS + ["batch_id", "silver_loaded_at"]
    if name not in spark.table(SILVER).columns
]
assert not missing_target, f"Missing Silver columns: {missing_target}"

upper_watermark = bronze_df.selectExpr(
    "max(bronze_loaded_at) AS newest"
).first()["newest"]

print("Previous watermark:", last_watermark)
print("Upper boundary:", upper_watermark)

if upper_watermark is None or upper_watermark <= last_watermark:
    dbutils.notebook.exit("NO_DATA: no new arrivals for " + PIPELINE)

# COMMAND ----------

# 4. Process the arriving records.
# One try/except stops the pipeline on failure and retains the checkpoint.
source_count = None
silver_count = None
rejected_count = None

try:
    incremental_df = bronze_df.filter(
        (col("bronze_loaded_at") > lit(last_watermark)) &
        (col("bronze_loaded_at") <= lit(upper_watermark))
    )
    source_count = incremental_df.count()

    # 5. Clean text and safely convert data types.
    clean_df = (
        incremental_df
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("categories", trim(col("categories")))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("_raw_unit_price", col("unit_price"))
        .withColumn("unit_price", expr("try_cast(`unit_price` AS DOUBLE)"))
        .withColumn("_raw_manufacturing_cost", col("manufacturing_cost"))
        .withColumn("manufacturing_cost", expr("try_cast(`manufacturing_cost` AS DOUBLE)"))
        .withColumn("_raw_created_at", col("created_at"))
        .withColumn("created_at", expr("coalesce(try_cast(`created_at` AS TIMESTAMP), try_to_timestamp(cast(`created_at` AS STRING), 'dd-MM-yyyy HH:mm:ss'), try_to_timestamp(cast(`created_at` AS STRING), 'dd-MM-yyyy HH:mm'), try_to_timestamp(cast(`created_at` AS STRING), 'dd-MM-yyyy'))"))
    )

    # 6. Mark invalid records. First applicable reason wins.
    checked_df = clean_df.withColumn(
        "error_reason",
        when(col("product_id").isNull() | (col("product_id") == ""), "Missing product_id")
        .when(col("product_name").isNull() | (col("product_name") == ""), "Missing product_name")
        .when(col("_raw_unit_price").isNotNull() & col("unit_price").isNull(), "Invalid unit_price format")
        .when(col("_raw_manufacturing_cost").isNotNull() & col("manufacturing_cost").isNull(), "Invalid manufacturing_cost format")
        .when(col("_raw_created_at").isNotNull() & col("created_at").isNull(), "Invalid created_at format")
        .when(col("unit_price").isNull() | (col("unit_price") <= 0), "Unit price must be positive")
        .when(col("manufacturing_cost") < 0, "Negative manufacturing_cost")
    )

    # 7. Separate invalid rows and valid rows.
    rejected_df = checked_df.filter(col("error_reason").isNotNull())
    rejected_count = rejected_df.count()

    error_df = rejected_df.select(
        lit(batch_id).alias("batch_id"),
        lit(TABLE_NAME).alias("table_name"),
        col(KEY).alias("record_id"),
        "error_reason",
        lit(start_time).alias("error_time")
    )

    if rejected_count > 0:
        error_df.write.format("delta").mode("append").saveAsTable(ERRORS)

    # The full rejected source rows remain in Bronze.
    raw_columns = ["_raw_unit_price","_raw_manufacturing_cost","_raw_created_at"]
    valid_df = (
        checked_df.filter(col("error_reason").isNull())
        .drop("error_reason", *raw_columns)
        .dropDuplicates()
    )

    # 8. Keep the newest valid arrival for each business key.
    # Conflicting versions at the same arrival time require source ordering.
    conflicts_df = (
        valid_df.groupBy(KEY, "bronze_loaded_at").count()
        .filter(col("count") > 1)
    )
    assert conflicts_df.limit(1).count() == 0, (
        "Conflicting versions share the same key and arrival timestamp."
    )

    latest_first = Window.partitionBy(KEY).orderBy(
        col("bronze_loaded_at").desc()
    )
    merge_df = (
        valid_df.withColumn("_row_number", row_number().over(latest_first))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
        .withColumn("batch_id", lit(batch_id))
        .withColumn("silver_loaded_at", lit(start_time))
    )
    silver_count = merge_df.count()
    versions_removed = source_count - rejected_count - silver_count

    duplicates_in_silver = (
        spark.table(SILVER).groupBy(KEY).count()
        .filter(col("count") > 1)
    )
    assert duplicates_in_silver.limit(1).count() == 0, (
        "Existing Silver contains duplicate business keys."
    )

    # 9. Build the explicit MERGE column lists.
    # Reuse lineage columns only when both tables already contain them.
    load_columns = BUSINESS_COLUMNS + ["batch_id", "silver_loaded_at"]
    target_columns = spark.table(SILVER).columns
    for name in ["source_file", "bronze_loaded_at"]:
        if name in merge_df.columns and name in target_columns:
            load_columns.append(name)

    update_list = ", ".join(
        f"target.`{name}` = source.`{name}`"
        for name in load_columns if name != KEY
    )
    insert_list = ", ".join(f"`{name}`" for name in load_columns)
    value_list = ", ".join(f"source.`{name}`" for name in load_columns)

    merge_df.createOrReplaceTempView("products_incremental_clean")

    # Existing ID -> UPDATE. New ID -> INSERT.
    spark.sql(f"""
    MERGE INTO {SILVER} AS target
    USING products_incremental_clean AS source
    ON target.`{KEY}` = source.`{KEY}`
    WHEN MATCHED THEN UPDATE SET {update_list}
    WHEN NOT MATCHED THEN INSERT ({insert_list})
    VALUES ({value_list})
    """)

    # 10. Log successful data processing.
    # silver_count is rows submitted to MERGE, not total target rows.
    spark.createDataFrame(
        [(batch_id, TABLE_NAME, start_time, datetime.now(),
          source_count, silver_count, rejected_count, "SUCCESS", None)],
        """batch_id STRING, table_name STRING, start_time TIMESTAMP,
           end_time TIMESTAMP, source_count BIGINT, silver_count BIGINT,
           rejected_count BIGINT, status STRING, error_message STRING"""
    ).write.format("delta").mode("append").saveAsTable(AUDIT)

    # 11. Advance the checkpoint LAST, using the captured upper boundary.
    spark.createDataFrame(
        [(PIPELINE, upper_watermark, batch_id, start_time)],
        """pipeline_name STRING, last_processed_timestamp TIMESTAMP,
           last_batch_id STRING, updated_at TIMESTAMP"""
    ).createOrReplaceTempView("products_new_watermark")

    spark.sql(f"""
    MERGE INTO {WATERMARK} AS target
    USING products_new_watermark AS source
    ON target.pipeline_name = source.pipeline_name
    WHEN MATCHED THEN UPDATE SET
        target.last_processed_timestamp = source.last_processed_timestamp,
        target.last_batch_id = source.last_batch_id,
        target.updated_at = source.updated_at
    """)

    print("SUCCESS:", PIPELINE)
    print("Source rows:", source_count)
    print("Rows submitted to MERGE:", silver_count)
    print("Rejected rows:", rejected_count)
    print("Duplicate/older valid versions removed:", versions_removed)

except Exception as error:
    # Do not advance the watermark on failure.
    # Record failure without masking the original exception.
    try:
        spark.createDataFrame(
            [(batch_id, TABLE_NAME, start_time, datetime.now(),
              source_count, None, None, "FAILED", str(error))],
            """batch_id STRING, table_name STRING, start_time TIMESTAMP,
               end_time TIMESTAMP, source_count BIGINT, silver_count BIGINT,
               rejected_count BIGINT, status STRING, error_message STRING"""
        ).write.format("delta").mode("append").saveAsTable(AUDIT)
    except Exception as audit_error:
        print("Failure could not be written to audit:", audit_error)
    raise

# COMMAND ----------

# 12. Inspect this run.
display(spark.table(WATERMARK).filter(col("pipeline_name") == PIPELINE))
display(spark.table(AUDIT).filter(col("batch_id") == batch_id))
display(spark.table(ERRORS).filter(col("batch_id") == batch_id))
