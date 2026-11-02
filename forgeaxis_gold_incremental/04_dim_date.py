# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ForgeAxis: extend the Gold calendar BEFORE loading facts.
# dim_date uses YYYYMMDD date_key, not an identity column.
# Existing dates and their audit metadata are preserved.

# COMMAND ----------

# 1. Configuration
from uuid import uuid4
from datetime import date
from pyspark.sql.functions import col, lit

GOLD = "forgeaxis.forgeaxis_gold.dim_date"

# Same starting range as the original full load.
# Extend these bounds when a new batch contains dates outside this range.
START_DATE = "2025-01-01"
END_DATE = "2027-12-31"

date.fromisoformat(START_DATE)
date.fromisoformat(END_DATE)
if START_DATE > END_DATE:
    raise ValueError("START_DATE must be before or equal to END_DATE.")

batch_id = "gold_calendar_" + str(uuid4())
run_time = spark.sql("SELECT current_timestamp() AS run_time").first()["run_time"]

# COMMAND ----------

# 2. Validate the existing calendar, without recreating it.
existing_df = spark.table(GOLD)
if existing_df.filter(col("date_key").isNull() | col("full_date").isNull()).limit(1).count() > 0:
    raise ValueError("Existing dim_date has null keys or dates.")
if existing_df.groupBy("date_key").count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("Existing dim_date has duplicate date keys.")
if existing_df.groupBy("full_date").count().filter(col("count") > 1).limit(1).count() > 0:
    raise ValueError("Existing dim_date has duplicate dates.")
if existing_df.filter(
    "date_key != CAST(date_format(full_date, 'yyyyMMdd') AS INT)"
).limit(1).count() > 0:
    raise ValueError("Existing dim_date has date keys inconsistent with full_date.")

# COMMAND ----------

# 3. Build one row for EVERY day in the configured range.
# sequence creates the date array; explode turns it into rows.
calendar_df = spark.sql(f"""
SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    dayofmonth(full_date) AS day,
    month(full_date) AS month,
    quarter(full_date) AS quarter,
    year(full_date) AS year
FROM (
    SELECT explode(
        sequence(DATE '{START_DATE}', DATE '{END_DATE}', INTERVAL 1 DAY)
    ) AS full_date
) calendar
""")

calendar_df = (
    calendar_df
    .withColumn("batch_id", lit(batch_id))
    .withColumn("gold_loaded_at", lit(run_time).cast("timestamp"))
)
calendar_df.createOrReplaceTempView("gold_calendar_source")

# COMMAND ----------

# 4. Insert only missing calendar dates.
# Repeated runs with the same range insert nothing.
display(spark.sql(f"""
MERGE INTO {GOLD} t
USING gold_calendar_source s
ON t.date_key = s.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, day, month, quarter, year, batch_id, gold_loaded_at
) VALUES (
    s.date_key, s.full_date, s.day, s.month, s.quarter, s.year,
    s.batch_id, s.gold_loaded_at
)
"""))

# COMMAND ----------

# 5. Show newly inserted dates.
display(spark.table(GOLD).filter(col("batch_id") == batch_id))
print("Calendar ready:", START_DATE, "to", END_DATE)
# No watermark: this dimension is generated from a range, not Silver arrivals.
