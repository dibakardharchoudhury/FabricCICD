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
│   ├── NB_00_Setup_Environment.py  — Verify lakehouses, check Spark config
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

### Step 1 — Prerequisites
- Fabric capacity (F2+) provisioned
- GitHub repository: `fabric-pab-cicd` with `develop` and `main` branches
- Fabric admin switches: Git integration and GitHub integration enabled

### Step 2 — Workspace Setup (script or UI)
```bash
pip install requests
# Edit TENANT_ID, CLIENT_ID, CLIENT_SECRET, CAPACITY_ID in the script
python scripts/01_setup_fabric_workspaces.py
```
Creates: PAB-Dev, PAB-Test, PAB-Prod with Bronze_LH, Silver_LH, Gold_LH in each.

### Step 3 — Upload Notebooks to PAB-Dev
In Fabric UI: PAB-Dev → + New Item → Import Notebook → upload each .py file from /notebooks/

### Step 4 — Run Notebooks in Order
1. NB_00_Setup_Environment  — verify connectivity
2. NB_01_Seed_Bronze        — create Delta tables with sample data
3. NB_02_Transform_Silver   — Bronze → Silver
4. NB_03_Aggregate_Gold     — Silver → Gold

### Step 5 — Connect Git Integration
```bash
# Edit GitHub PAT and repo URL in the script
python scripts/02_connect_git_integration.py
```
Or manually: PAB-Dev → Workspace Settings → Git Integration → GitHub

### Step 6 — Commit to GitHub
In PAB-Dev: Source Control icon → Select all items → Add commit message → Commit

### Step 7 — Create Deployment Pipeline
```bash
python scripts/03_create_deployment_pipeline.py
```
Note the pipeline ID — add to GitHub secrets as `FABRIC_PIPELINE_ID`

### Step 8 — Configure GitHub Actions
Add these secrets to your GitHub repository:
- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`
- `FABRIC_PIPELINE_ID`

Copy `github_actions/*.yml` to `.github/workflows/` in your repo.

### Step 9 — Demo the CI/CD Loop
1. Open `NB_02_Transform_Silver` in PAB-Dev
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
