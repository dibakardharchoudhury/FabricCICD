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

# PARAMETERS CELL ********************

refresh_semantic_model = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# CELL ********************

# Cell 6 — Validate Gold and refresh the Direct Lake model
from sempy.fabric._client._rest_client import PowerBIRestClient

def _powerbi_base_url(self):
    return "https://api.powerbi.com/"

PowerBIRestClient._get_default_base_url = _powerbi_base_url

import sempy_labs as labs
from sempy_labs import directlake
import notebookutils
import time

_EXPECTED_TABLES = ["production_daily", "cost_monthly", "schedule_summary", "field_kpi_facts"]

# Resolve the current workspace at runtime.
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId") or spark.conf.get("trident.workspace.id")
if not _ws_id:
    raise Exception("Could not resolve the current workspace id — cannot refresh Gold_SM.")

print("\n── Gold Layer Validation ────────────────────────────")
for _tbl in _EXPECTED_TABLES:
    print(f"  Gold_LH.{_tbl}: {spark.table(f'Gold_LH.dbo.{_tbl}').count()} rows")

_SEMANTIC_MODEL = "Gold_SM"
_MAX_ATTEMPTS   = 15
_BACKOFF_SECS   = 120

# Rebind defensively before refreshing.
_GOLD_LAKEHOUSE = "Gold_LH"
_gold_lh_meta = notebookutils.lakehouse.get(_GOLD_LAKEHOUSE, _ws_id)
_gold_lh_id   = _gold_lh_meta["id"] if isinstance(_gold_lh_meta, dict) else _gold_lh_meta.id
directlake.update_direct_lake_model_connection(
    dataset=_SEMANTIC_MODEL, workspace=_ws_id,
    source=_gold_lh_id, source_type="Lakehouse",
    source_workspace=_ws_id, use_sql_endpoint=False,
)
print(f"🔗 '{_SEMANTIC_MODEL}' Direct Lake connection re-pointed to '{_GOLD_LAKEHOUSE}' "
      f"({_gold_lh_id}) in this workspace before refresh.")

if refresh_semantic_model:
    print(f"\n── Refreshing Direct Lake (on OneLake) model '{_SEMANTIC_MODEL}' (full reframe) ──")
    print(f"Semantic Link Power BI endpoint: {PowerBIRestClient().default_base_url}")
else:
    print(f"⏭️ '{_SEMANTIC_MODEL}' refresh skipped by the caller.")

_refreshed = not refresh_semantic_model
for _attempt in range(1, _MAX_ATTEMPTS + 1) if refresh_semantic_model else ():
    try:
        labs.refresh_semantic_model(
            dataset=_SEMANTIC_MODEL, workspace=_ws_id, refresh_type="full",
        )
        print(f"✅ '{_SEMANTIC_MODEL}' refreshed on attempt {_attempt}/{_MAX_ATTEMPTS} "
              f"(Direct Lake reframe complete) — report is up to date.")
        _refreshed = True
        break
    except Exception as _e:
        _msg = str(_e)
        _forbidden = "403 Forbidden" in _msg
        _transient = not _forbidden and (
            ("0xC14700DF" in _msg) or ("do not exist or access" in _msg.lower())
        )
        if _transient and _attempt < _MAX_ATTEMPTS:
            print(f"⏳ Attempt {_attempt}/{_MAX_ATTEMPTS}: tables still syncing into metadata after "
                  f"create-from-scratch; retrying in {_BACKOFF_SECS}s "
                  f"(elapsed wait so far ~{(_attempt - 1) * _BACKOFF_SECS // 60} min)...")
            time.sleep(_BACKOFF_SECS)
            continue

        _failure_guidance = (
            "  • The Power BI API returned a non-retryable 403. Semantic-model refresh is not in\n"
            "    the supported Semantic Link function subset for service-principal-triggered runs."
            if _forbidden else
            "  • 0xC14700DF / 'do not exist or access' can indicate create-from-scratch metadata sync.\n"
            "    Extend _MAX_ATTEMPTS or _BACKOFF_SECS if the first-run sync exceeds this retry window."
        )
        raise Exception(
            f"Refresh of Direct Lake (on OneLake) model '{_SEMANTIC_MODEL}' FAILED after "
            f"{_attempt} attempt(s) (~{((_attempt - 1) * _BACKOFF_SECS) // 60} min of waiting): {_e}\n"
            f"{_failure_guidance}"
        ) from _e

_completion = (
    "tables written and Gold_SM refreshed"
    if _refreshed and refresh_semantic_model
    else "tables written and Gold_SM rebound"
)
print(f"🏆 Gold aggregation COMPLETE — {_completion}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
