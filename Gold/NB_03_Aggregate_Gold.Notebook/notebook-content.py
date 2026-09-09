# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "663f3fff-65c9-485e-ae23-fd03018fc613",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "3f0dbdcc-2520-4432-bbb2-c7a53375c73c",
# META       "known_lakehouses": [
# META         {
# META           "id": "663f3fff-65c9-485e-ae23-fd03018fc613"
# META         },
# META         {
# META           "id": "9c0b5838-532b-4a99-9d34-29cd1576d7a7"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "86313016-e213-a285-4d09-9801e4f0072b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Aggregates Silver data into report-ready Gold tables.

# Cell 1 — Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, max as _max,
    round as _round, date_format, current_timestamp, lit
)
spark = SparkSession.builder.getOrCreate()
print("Starting Gold aggregation...")

# Preserve Delta table identity unless the schema changed.
def _write_gold(df, table):
    full = f"Gold_LH.dbo.{table}"
    if spark.catalog.tableExists(full):
        try:
            df.write.format("delta").mode("overwrite").saveAsTable(full)
            return
        except Exception:
            pass
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)

# Cell 2 — Gold Production: daily field summary
df_silver_prod = spark.table("Silver_LH.dbo.production_conformed")

df_gold_prod = (
    df_silver_prod
    .groupBy("date", "field")
    .agg(
        _sum("oil_bbl").alias("total_oil_bbl"),
        _sum("gas_mcf").alias("total_gas_mcf"),
        _sum("water_bbl").alias("total_water_bbl"),
        _sum("boe_total").alias("total_boe"),
        avg("water_cut_pct").alias("avg_water_cut_pct"),
        count(lit(1)).alias("active_well_count"),
    )
    .withColumn("total_oil_bbl", _round("total_oil_bbl", 2))
    .withColumn("total_gas_mcf", _round("total_gas_mcf", 2))
    .withColumn("total_boe", _round("total_boe", 2))
    .withColumn("avg_water_cut_pct", _round("avg_water_cut_pct", 2))
    .withColumn("report_generated_at", current_timestamp())
    .orderBy("date", "field")
)

_write_gold(df_gold_prod, "production_daily")

print(f"✅ Gold_LH.production_daily: {df_gold_prod.count()} rows")

# Cell 3 — Gold Cost: monthly field + cost type summary
df_silver_cost = spark.table("Silver_LH.dbo.cost_conformed")

df_gold_cost = (
    df_silver_cost
    .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    .groupBy("year_month", "field", "cost_type")
    .agg(
        _sum("amount_usd").alias("total_cost_usd"),
        avg("cost_per_boe").alias("avg_cost_per_boe"),
        count("cost_id").alias("transaction_count"),
        _max("amount_usd").alias("max_single_cost_usd"),
    )
    .withColumn("total_cost_usd",       _round("total_cost_usd", 2))
    .withColumn("avg_cost_per_boe",     _round("avg_cost_per_boe", 4))
    .withColumn("report_generated_at",  current_timestamp())
)

_write_gold(df_gold_cost, "cost_monthly")

print(f"✅ Gold_LH.cost_monthly: {df_gold_cost.count()} rows")

# Cell 4 — Gold Schedule: field activity summary
df_silver_sched = spark.table("Silver_LH.dbo.schedule_conformed")

df_gold_sched = (
    df_silver_sched
    .groupBy("field", "priority", "status")
    .agg(
        count("schedule_id").alias("activity_count"),
        avg("duration_days").alias("avg_duration_days"),
        _sum(col("is_critical").cast("int")).alias("critical_count"),
        _sum(col("is_in_progress").cast("int")).alias("in_progress_count"),
    )
    .withColumn("avg_duration_days",    _round("avg_duration_days", 1))
    .withColumn("report_generated_at",  current_timestamp())
)

_write_gold(df_gold_sched, "schedule_summary")

print(f"✅ Gold_LH.schedule_summary: {df_gold_sched.count()} rows")

# Cell 5 — Build the schema-stable cross-domain KPI table
df_prod_summary = (
    df_gold_prod
    .groupBy("field")
    .agg(
        avg("total_boe").alias("avg_daily_boe"),
        _sum("total_oil_bbl").alias("cumulative_oil_bbl"),
        avg("active_well_count").alias("avg_active_wells"),
    )
)

df_cost_summary = (
    spark.table("Gold_LH.dbo.cost_monthly")
    .groupBy("field")
    .agg(
        _sum("total_cost_usd").alias("total_cost_usd"),
        avg("avg_cost_per_boe").alias("avg_cost_per_boe"),
    )
)

df_kpi_facts = (
    df_prod_summary
    .join(df_cost_summary, "field", "left")
    .withColumn("avg_daily_boe",       _round("avg_daily_boe", 2))
    .withColumn("cumulative_oil_bbl",  _round("cumulative_oil_bbl", 2))
    .withColumn("total_cost_usd",      _round("total_cost_usd", 2))
    .withColumn("avg_cost_per_boe",    _round("avg_cost_per_boe", 4))
    .withColumn("report_generated_at", current_timestamp())
)

_write_gold(df_kpi_facts, "field_kpi_facts")

print(f"✅ Gold_LH.field_kpi_facts: {df_kpi_facts.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
