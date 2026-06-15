# Microsoft Fabric CI/CD Demo — Medallion Architecture

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
   Gold_SM (Semantic Model, DirectLake)
   Gold_Dashboard (Power BI Report)
          ↓
🚀 CI/CD
   GitHub (develop branch) ← ws-CICD-Dev workspace
   Deployment Pipeline: ws-CICD-Dev → ws-CICD-Prod
   GitHub Actions: deploy on merge to main
```

## Files

```
fabric-cicd-demo/
├── sample_data/
│   ├── production_data.csv     — Simulates PIMS (well production volumes)
│   ├── cost_data.csv           — Simulates Alpha (OPEX/CAPEX costs)
│   └── schedule_data.csv       — Simulates SharePoint (maintenance schedule)
│
├── NB_00_Setup_Environment.Notebook/  — Verify the three lakehouses exist in the workspace
├── NB_01_Seed_Bronze.Notebook/        — Create Bronze Delta tables from sample data
├── NB_02_Transform_Silver.Notebook/   — Clean & enrich → Silver layer
├── NB_03_Aggregate_Gold.Notebook/     — Aggregate → Gold layer + KPI facts table
│     (each is a Fabric Git folder: .platform + notebook-content.py)
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
│   ├── Gold_SM.bim               — TMSL model definition (4 tables, 15+ measures)
│   └── definition.pbism            — Fabric semantic model settings
│
├── github_actions/
│   └── deploy-dev-to-prod.yml      — Promote Dev → Prod (reviewer-approved)
│
└── deployment_rules/
    └── deployment_rules.json       — Deployment rules reference (configure in UI)
```

## Three things are called “pipeline” — don’t mix them up

This demo uses the word “pipeline” in three different ways. Knowing which is which
makes every step below clear:

| # | Name | Lives in | What it does | When you run it |
|---|------|----------|--------------|-----------------|
| 1 | **Data pipelines** | `pipelines/*.json` | Fabric Data Factory pipelines that move/transform *data* inside a workspace | During the data build / refresh (Steps 5 & 10) |
| 2 | **Fabric Deployment Pipeline** | Fabric portal → Deployment pipelines | Promotes *items* (lakehouses, notebooks, pipelines, semantic model) across **Dev → Prod** workspaces | During CI/CD setup (Step 8) and each promotion |
| 3 | **GitHub Actions “pipelines”** | `github_actions/*.yml` | CI/CD automation that triggers the Fabric Deployment Pipeline on push/merge | Automatically on git push (Steps 9 & 11) |

### The two data pipelines (#1 above)
- **`PL_Refresh_Master`** — *orchestration.* Runs the whole Medallion build in order:
  `NB_01_Seed_Bronze → NB_02_Transform_Silver → NB_03_Aggregate_Gold → Refresh Gold_SM`,
  with each step gated on the previous one succeeding. **Run this for a one-click full
  refresh** — especially after a deployment, because lakehouse deploys carry structure
  but no data. It is parameterized by `Environment` / `WorkspaceId` / `SemanticModelId`
  so the *same* pipeline works in Dev and Prod.
- **`PL_Copy_Bronze_Ingest`** — *ingestion.* A Copy Activity pipeline that loads the raw
  CSVs from OneLake Files into the Bronze Delta tables (with column mappings and an
  incremental watermark). It is the “real-world ingestion” alternative to
  `NB_01_Seed_Bronze`, which fakes Bronze data with inline rows for a zero-dependency demo.

> **Which do I run to load data?**
> - Simplest demo → just run the **notebooks** `NB_01`→`NB_03` (no pipeline needed).
> - One-click refresh / post-deployment → run **`PL_Refresh_Master`**.
> - To show file-based ingestion → run **`PL_Copy_Bronze_Ingest`** (then Silver/Gold).

## Quick Start

> The fastest path to a working demo is to run the **data pipeline first** (Steps 1–5)
> to prove the Medallion build, then layer on **Git + Deployment Pipelines** (Steps 6–11)
> to demonstrate CI/CD. You can stop after Step 5 for a pure data demo.

### Step 1 — Prerequisites
- Fabric capacity (F2+) provisioned and assigned to your workspace
- Two Fabric workspaces (this demo uses **`ws-CICD-Dev`** and **`ws-CICD-Prod`** —
  substitute your own names). Build everything in `ws-CICD-Dev`; `ws-CICD-Prod` is the
  promotion target
- For the CI/CD half: a GitHub repo with `develop` and `main` branches, and the
  Fabric admin switches **Git integration** and **GitHub integration** enabled

### Step 2 — Create the three lakehouses
In your dev workspace (`ws-CICD-Dev`), create three lakehouses with these **exact** names:
- `Bronze_LH`
- `Silver_LH`
- `Gold_LH`

> The notebooks write tables with three-part names (e.g. `Bronze_LH.dbo.production_raw`),
> so the lakehouse names must match exactly.

### Step 3 — Sync the notebooks from Git
The notebooks are stored in Fabric Git format (each as an `NB_*.Notebook/` folder).
Connect the workspace to this repo (**Workspace settings → Git integration**), then
**Source control → Update** to bring the notebooks into the workspace. No manual
`.py` import is needed.

### Step 4 — Attach the lakehouses to the notebooks ⚠️
Fabric notebooks reference attached lakehouses by **runtime object GUID**, and those
GUIDs are unique to each workspace and change whenever a workspace/lakehouse is
recreated. They are therefore **not** committed to Git — the notebooks ship with an
empty `"dependencies": {}` block. You must attach the lakehouses **once per workspace**
after every Git sync / deployment (Dev, Prod):

Open each notebook (`NB_01`, `NB_02`, `NB_03`) and in the **Explorer → Lakehouses**
pane add **all three** lakehouses (`Bronze_LH`, `Silver_LH`, `Gold_LH`), then set
**one as the default**. Without this, `saveAsTable("Silver_LH.…")` and cross-lakehouse
reads fail with `[SCHEMA_NOT_FOUND]`. (`NB_00` documents this step inline.)

> ⚠️ **Do not commit the attached GUIDs back to Git.** If you commit from the workspace
> after attaching, re-strip the `dependencies` block (or just don't stage those metadata
> lines) so the repo stays workspace-portable.

`NB_00_Setup_Environment` needs no lakehouse — it only verifies the lakehouses exist
in the workspace it runs in. Open it and confirm `WORKSPACE_NAME` matches your
workspace before running.

### Step 5 — Run the notebooks in order
1. `NB_00_Setup_Environment` — confirms `Bronze_LH`, `Silver_LH`, `Gold_LH` exist
2. `NB_01_Seed_Bronze`        — writes Bronze Delta tables from inline sample data
3. `NB_02_Transform_Silver`   — Bronze → Silver (clean, conform, KPIs)
4. `NB_03_Aggregate_Gold`     — Silver → Gold (aggregates + KPI fact table)

After Step 5 you have a fully built Medallion lakehouse. Stop here for a data-only
demo, or continue to wire up CI/CD.

> **Pipeline alternative (optional):** instead of running the four notebooks by hand,
> you can import `pipelines/PL_Refresh_Master.json` and run it once — it executes
> `NB_01 → NB_02 → NB_03` and refreshes the semantic model in a single click. To
> demonstrate file-based ingestion instead of the seed notebook, run
> `pipelines/PL_Copy_Bronze_Ingest.json` first, then `NB_02` and `NB_03`.

### Step 6 — Connect Git Integration
In the workspace: **Workspace settings → Git integration → GitHub** — select the repo,
branch, and folder, then connect.

### Step 7 — Commit to GitHub
In the workspace: **Source control** icon → select all items → add a commit message → **Commit**

### Step 8 — Create Deployment Pipeline
In the Fabric portal: **Workspaces → Deployment pipelines → New pipeline**. Create two
stages (Development → Production) and assign `ws-CICD-Dev` to Development and
`ws-CICD-Prod` to Production. This **Fabric Deployment Pipeline** (pipeline type #2)
promotes items across Dev → Prod. Note the pipeline ID — add it to GitHub secrets as
`FABRIC_PIPELINE_ID`.

### Step 9 — Configure GitHub Actions
Add these secrets to your GitHub repository:
- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`
- `FABRIC_PIPELINE_ID`

Copy `github_actions/*.yml` to `.github/workflows/` in your repo.

### Step 10 — Refresh data after deployment
Lakehouse deployments copy **structure only, not data**. After the pipeline deploys
to Prod:
1. **Attach the lakehouses** in the target workspace — open `NB_01` / `NB_02` / `NB_03`
   and add all three lakehouses (see Step 4). Deployed notebooks arrive with an empty
   `dependencies` block, so they need their lakehouses re-attached in that workspace
   before they run.
2. **Repopulate** the target workspace by either:
   - running the notebooks `NB_01` → `NB_02` → `NB_03` in that workspace, **or**
   - running the **`PL_Refresh_Master`** data pipeline once (does all three + semantic
     model refresh in one click — the recommended option for Prod).

### Step 11 — Demo the CI/CD Loop
1. Open `NB_02_Transform_Silver` in your dev workspace (`ws-CICD-Dev`)
2. Uncomment the `gor_ratio` line (Gas-to-Oil Ratio KPI)
3. Run the notebook — verify the new column appears
4. Source Control → Commit: `feat: add gas-to-oil ratio KPI`
5. GitHub: open PR from develop → main → review the diff → merge
6. GitHub Actions runs `deploy-dev-to-prod.yml` → waits for reviewer approval on the
   `production` environment, then promotes Dev → Prod

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
| Notebook | Attached-lakehouse GUIDs are workspace-specific and change on recreate | Keep `dependencies` empty in Git; re-attach lakehouses in each workspace (Step 4) |
