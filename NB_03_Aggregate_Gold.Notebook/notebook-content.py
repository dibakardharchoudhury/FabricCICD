# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2ca2ea6-4043-4e94-9984-aa1c29742578",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "292e18c3-b95e-42d1-bb02-9a2064fee5b8",
# META       "known_lakehouses": [
# META         {
# META           "id": "d2ca2ea6-4043-4e94-9984-aa1c29742578"
# META         },
# META         {
# META           "id": "fd49e2e8-3e64-4e59-9556-2ea23468552a"
# META         },
# META         {
# META           "id": "4e39b97a-c57e-4ca2-8615-fa2b93682001"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: NB_03_Aggregate_Gold
# Purpose: Aggregate Silver → Gold layer (report-ready, pre-computed KPIs)
# Layer: Gold — report-oriented, aggregated, enriched
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, max as _max, min as _min,
    round as _round, date_format, current_timestamp, lit
)
spark = SparkSession.builder.getOrCreate()
print("Starting Gold aggregation...")

# Cell 2 — Gold Production: daily field-level summary
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
        count("well_id").alias("active_well_count"),
    )
    .withColumn("total_oil_bbl",        _round("total_oil_bbl", 2))
    .withColumn("total_gas_mcf",        _round("total_gas_mcf", 2))
    .withColumn("total_boe",            _round("total_boe", 2))
    .withColumn("avg_water_cut_pct",    _round("avg_water_cut_pct", 2))
    .withColumn("report_generated_at",  current_timestamp())
)

df_gold_prod.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_LH.dbo.production_daily")

print(f"✅ Gold_LH.production_daily: {df_gold_prod.count()} rows")
display(df_gold_prod.orderBy("date", "field"))

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

df_gold_cost.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_LH.dbo.cost_monthly")

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

df_gold_sched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_LH.dbo.schedule_summary")

print(f"✅ Gold_LH.schedule_summary: {df_gold_sched.count()} rows")

# Cell 5 — Cross-domain enrichment: KPI fact table for reporting
df_prod_summary = (
    spark.table("Gold_LH.dbo.production_daily")
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

df_kpi_facts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_LH.dbo.field_kpi_facts")

print(f"✅ Gold_LH.field_kpi_facts: {df_kpi_facts.count()} rows")
display(df_kpi_facts)

# Cell 6 — Final validation
print("\n── Gold Layer Validation ────────────────────────────")
for tbl in ["production_daily", "cost_monthly", "schedule_summary", "field_kpi_facts"]:
    n = spark.table(f"Gold_LH.dbo.{tbl}").count()
    print(f"  Gold_LH.{tbl}: {n} rows")

print("\n🏆 Gold aggregation COMPLETE — tables ready for Semantic Model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
