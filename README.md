# Microsoft Fabric CI/CD Demo — PAB Medallion Architecture

End-to-end CI/CD demonstration with Medallion Lakehouse, GitHub integration, and Fabric Deployment Pipelines.

## Architecture

```
Sources (PIMS / Alpha / SharePoint)
          ↓
🟤 BRONZE LAYER (source-oriented)
   Bronze_LH  ← PL_Copy_Bronze_Ingest + DF_Bronze_Production/Cost/Schedule
          ↓
⚙️  SILVER LAYER (domain-oriented)
   Silver_LH  ← NB_02_Transform_Silver + DF_Silver_PA/Cost/Schedule
          ↓
🥇 GOLD LAYER (report-oriented)
   Gold_LH    ← NB_03_Aggregate_Gold + DF_Gold_PA/Cost/Schedule
   PAB_Gold_SM (Semantic Model, DirectLake)
   PAB_Dashboard (Power BI Report)
          ↓
🚀 CI/CD
   GitHub (develop branch) ← PAB-Dev workspace
   Deployment Pipeline: PAB-Dev → PAB-Test → PAB-Prod
   GitHub Actions: auto-deploy on push to develop
```

## Files

```
fabric-cicd-demo/
├── sample_data/
│   ├── production_data.csv     — Simulates PIMS (well production volumes)
│   ├── cost_data.csv           — Simulates Alpha (OPEX/CAPEX costs)
│   └── schedule_data.csv       — Simulates SharePoint (maintenance schedule)
│
├── notebooks/
│   ├── NB_00_Setup_Environment.py  — Verify the three lakehouses exist in the workspace
│   ├── NB_01_Seed_Bronze.py        — Create Bronze Delta tables from sample data
│   ├── NB_02_Transform_Silver.py   — Clean & enrich → Silver layer
│   └── NB_03_Aggregate_Gold.py     — Aggregate → Gold layer + KPI facts table
│
├── pipelines/
│   ├── PL_Refresh_Master.json      — Master orchestration (Bronze→Silver→Gold→Refresh SM)
│   └── PL_Copy_Bronze_Ingest.json  — Copy Activity pipeline for file ingestion
│
├── dataflows/
│   ├── DF_Bronze_Production.m      — Power Query M: SharePoint → Bronze_LH
│   ├── DF_Silver_PA.m              — Power Query M: Bronze → Silver production
│   └── DF_Gold_PA.m                — Power Query M: Silver → Gold aggregation
│
├── semantic_model/
│   ├── PAB_Gold_SM.bim             — TMSL model definition (4 tables, 15+ measures)
│   └── definition.pbism            — Fabric semantic model settings
│
├── github_actions/
│   ├── deploy-dev-to-test.yml      — Auto-deploy on push to develop branch
│   └── deploy-test-to-prod.yml     — Manual production deployment with approval
│
├── scripts/
│   ├── 01_setup_fabric_workspaces.py   — Create workspaces + lakehouses via REST API
│   ├── 02_connect_git_integration.py   — Connect PAB-Dev to GitHub via REST API
│   └── 03_create_deployment_pipeline.py — Create pipeline + assign workspaces
│
└── deployment_rules/
    └── deployment_rules.json       — Deployment rules reference (configure in UI)
```

## Quick Start

> The fastest path to a working demo is to run the **data pipeline first** (Steps 1–5)
> to prove the Medallion build, then layer on **Git + Deployment Pipelines** (Steps 6–11)
> to demonstrate CI/CD. You can stop after Step 5 for a pure data demo.

### Step 1 — Prerequisites
- Fabric capacity (F2+) provisioned and assigned to your workspace
- A Fabric workspace (this demo uses **`ws-CICD-DevTest`** — substitute your own name)
- For the CI/CD half: a GitHub repo with `develop` and `main` branches, and the
  Fabric admin switches **Git integration** and **GitHub integration** enabled

### Step 2 — Create the three lakehouses
In your workspace (`ws-CICD-DevTest`), create three lakehouses with these **exact** names:
- `Bronze_LH`
- `Silver_LH`
- `Gold_LH`

> The notebooks write tables with two-part names (e.g. `Bronze_LH.production_raw`),
> so the lakehouse names must match exactly.
>
> *(Optional, multi-workspace path)* To provision `PAB-Dev / PAB-Test / PAB-Prod`
> automatically via REST API instead, edit `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`,
> `CAPACITY_ID` and run `pip install requests` then
> `python scripts/01_setup_fabric_workspaces.py`.

### Step 3 — Import the notebooks
In the Fabric UI: workspace → **+ New item → Import notebook** → upload each `.py`
file from `/notebooks/`.

### Step 4 — Attach all three lakehouses to every notebook ⚠️
Open each notebook (`NB_01`, `NB_02`, `NB_03`) and in the **Explorer / Lakehouses**
pane add **all three** lakehouses (`Bronze_LH`, `Silver_LH`, `Gold_LH`), then set
**one as the default**. Without this, `saveAsTable("Silver_LH.…")` and cross-lakehouse
reads will fail with `[SCHEMA_NOT_FOUND]`.

`NB_00_Setup_Environment` does not need a lakehouse attached — it only verifies the
lakehouses exist in the workspace it is running in. Open it and confirm
`WORKSPACE_NAME` matches your workspace before running.

### Step 5 — Run the notebooks in order
1. `NB_00_Setup_Environment` — confirms `Bronze_LH`, `Silver_LH`, `Gold_LH` exist
2. `NB_01_Seed_Bronze`        — writes Bronze Delta tables from inline sample data
3. `NB_02_Transform_Silver`   — Bronze → Silver (clean, conform, KPIs)
4. `NB_03_Aggregate_Gold`     — Silver → Gold (aggregates + KPI fact table)

After Step 5 you have a fully built Medallion lakehouse. Stop here for a data-only
demo, or continue to wire up CI/CD.

### Step 6 — Connect Git Integration
```bash
# Edit GitHub PAT and repo URL in the script
python scripts/02_connect_git_integration.py
```
Or manually: workspace → **Workspace settings → Git integration → GitHub**

### Step 7 — Commit to GitHub
In the workspace: **Source control** icon → select all items → add a commit message → **Commit**

### Step 8 — Create Deployment Pipeline
```bash
python scripts/03_create_deployment_pipeline.py
```
Note the pipeline ID — add it to GitHub secrets as `FABRIC_PIPELINE_ID`

### Step 9 — Configure GitHub Actions
Add these secrets to your GitHub repository:
- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`
- `FABRIC_PIPELINE_ID`

Copy `github_actions/*.yml` to `.github/workflows/` in your repo.

### Step 10 — Refresh data after deployment
Lakehouse deployments copy **structure only, not data**. After the pipeline deploys
to Test/Prod, run `NB_01` → `NB_02` → `NB_03` (or `PL_Refresh_Master`) in the target
workspace to populate tables.

### Step 11 — Demo the CI/CD Loop
1. Open `NB_02_Transform_Silver` in your dev workspace
2. Uncomment the `gor_ratio` line (Gas-to-Oil Ratio KPI)
3. Run the notebook — verify the new column appears
4. Source Control → Commit: `feat: add gas-to-oil ratio KPI`
5. GitHub: open PR from develop → main → review the diff → merge
6. GitHub Actions triggers automatically → deploys to Test
7. For production: manually trigger `deploy-test-to-prod.yml`

## Key Limitations to Know

| Area | Limitation | Impact |
|------|-----------|--------|
| Git | Sensitivity labels block commits | Remove labels before demo |
| Git | GitHub max 50MB per commit | Commit items in batches |
| Git | Only workspace Admin can connect/disconnect | Pre-configure before demo |
| Git | MyWorkspace cannot connect to Git | Always use named workspaces |
| Deploy | Items added after workspace assignment are NOT auto-paired | Build all items first, then assign workspaces |
| Deploy | Lakehouse deploys structure only (no data) | Run refresh pipeline after each deploy |
| Deploy | Semantic models require Enhanced Metadata (mandatory Feb 2026) | Always publish from modern PBI Desktop |
| Deploy | Same-name unpaired items create duplicates | Verify pairing in pipeline UI before deploying |
