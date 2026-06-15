# Fabric Notebook: NB_00_Setup_Environment
# Purpose: One-time setup — verifies lakehouses exist, uploads sample CSVs to OneLake
# Run this FIRST before any other notebook
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Parameters (edit these for your environment)
WORKSPACE_NAME = "PAB-Dev"           # Your Fabric workspace name
BRONZE_LH      = "Bronze_LH"        # Bronze Lakehouse name
SILVER_LH      = "Silver_LH"        # Silver Lakehouse name
GOLD_LH        = "Gold_LH"          # Gold Lakehouse name
ENVIRONMENT    = "dev"               # dev | test | prod — set via pipeline parameter

print(f"Environment: {ENVIRONMENT}")
print(f"Workspace:   {WORKSPACE_NAME}")

# Cell 2 — Verify Lakehouse connectivity
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

for lh in [BRONZE_LH, SILVER_LH, GOLD_LH]:
    try:
        tables = spark.catalog.listTables(lh)
        print(f"✅ {lh}: connected ({len(tables)} tables)")
    except Exception as e:
        print(f"❌ {lh}: {e}")
        print(f"   → Create lakehouse '{lh}' in Fabric before running this notebook")

# Cell 3 — Install / verify Delta support
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
print("✅ Delta Lake configured")
