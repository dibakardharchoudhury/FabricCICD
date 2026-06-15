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
