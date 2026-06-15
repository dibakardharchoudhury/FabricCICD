# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fd49e2e8-3e64-4e59-9556-2ea23468552a",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "292e18c3-b95e-42d1-bb02-9a2064fee5b8",
# META       "known_lakehouses": [
# META         {
# META           "id": "fd49e2e8-3e64-4e59-9556-2ea23468552a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: NB_00_Setup_Environment
# Purpose: One-time setup — verifies lakehouses exist, uploads sample CSVs to OneLake
# Run this FIRST before any other notebook
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Parameters (edit these for your environment)
WORKSPACE_NAME = "ws-CICD-DevTest"   # Your Fabric workspace name
BRONZE_LH      = "Bronze_LH"        # Bronze Lakehouse name
SILVER_LH      = "Silver_LH"        # Silver Lakehouse name
GOLD_LH        = "Gold_LH"          # Gold Lakehouse name
ENVIRONMENT    = "dev"               # dev | test | prod — set via pipeline parameter

print(f"Environment: {ENVIRONMENT}")
print(f"Workspace:   {WORKSPACE_NAME}")

# Cell 2 — Verify Lakehouse connectivity
from pyspark.sql import SparkSession
import notebookutils

spark = SparkSession.builder.getOrCreate()

# List the lakehouses that actually exist in the attached workspace.
# Using notebookutils avoids resolving the lakehouse name as a schema inside the
# default lakehouse (which causes [SCHEMA_NOT_FOUND] errors).
# Note: lakehouse.list() takes a workspace *ID* (GUID), not a display name, so we
# resolve the current workspace ID at runtime instead of passing WORKSPACE_NAME.
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
existing_lakehouses = {lh.displayName for lh in notebookutils.lakehouse.list(workspace_id)}

for lh in [BRONZE_LH, SILVER_LH, GOLD_LH]:
    if lh in existing_lakehouses:
        print(f"✅ {lh}: found in workspace '{WORKSPACE_NAME}'")
    else:
        print(f"❌ {lh}: not found in workspace '{WORKSPACE_NAME}'")
        print(f"   → Create lakehouse '{lh}' in Fabric before running this notebook")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
