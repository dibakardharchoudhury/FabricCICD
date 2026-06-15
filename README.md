# Microsoft Fabric CI/CD Demo — Medallion Architecture

End-to-end CI/CD demonstration with Medallion Lakehouse, GitHub integration, and Fabric Deployment Pipelines.

## Architecture

```
Sources (PIMS / Alpha / SharePoint)
          ↓
🟤 BRONZE LAYER (source-oriented)
   Bronze_LH  ← NB_01_Seed_Bronze
          ↓
⚙️  SILVER LAYER (domain-oriented)
   Silver_LH  ← NB_02_Transform_Silver
          ↓
🥇 GOLD LAYER (report-oriented)
   Gold_LH    ← DF_Gold_PA (production_daily) + NB_03_Aggregate_Gold (cost / schedule / KPIs)
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
│   ├── PL_Refresh_Master.json      — Single end-to-end orchestration (NB_01 → NB_02 → DF_Gold_PA → NB_03 → Refresh SM)
│   └── PL_Refresh_Master.zip       — Same pipeline as a Fabric Import template (Home → Import from a template)
│
├── dataflows/
│   ├── DF_Gold_PA.m              — Power Query M: Silver → Gold production_daily (the one Gold table NB_03 does not build)
│   └── DF_Gold_PA.pqt            — Same dataflow as a Power Query template (Get data → Import from a Power Query template)
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
| 1 | **Data pipeline** | `pipelines/PL_Refresh_Master.json` | One Fabric Data Factory pipeline that runs the whole Medallion build inside a workspace | During the data build / refresh (Steps 6 & 16) |
| 2 | **Fabric Deployment Pipeline** | Fabric portal → Deployment pipelines | Promotes *items* (lakehouses, notebooks, pipeline, dataflow, semantic model) across **Dev → Prod** workspaces | During CI/CD setup (Steps 12–13) and each promotion |
| 3 | **GitHub Actions “pipelines”** | `github_actions/*.yml` | CI/CD automation that triggers the Fabric Deployment Pipeline on push/merge | Automatically on git push (Steps 15 & 17) |

### The data pipeline (#1 above)
- **`PL_Refresh_Master`** — *the single end-to-end orchestration.* Runs the whole
  Medallion build in order:
  `NB_01_Seed_Bronze → NB_02_Transform_Silver → DF_Gold_PA → NB_03_Aggregate_Gold → Refresh Gold_SM`,
  with each step gated on the previous one succeeding. The **`DF_Gold_PA`** dataflow runs
  (as a built-in **Dataflow** activity) before `NB_03` because it builds the Gold
  `production_daily` table that `NB_03`'s KPI cell joins. **Run this one pipeline for a
  full, one-click end-to-end refresh** — especially after a deployment, because lakehouse
  deploys carry structure but no data. It carries an `Environment` parameter (`dev` by
  default) and resolves its workspace at run time via `@pipeline().DataFactory`, so the
  *same* pipeline works in Dev and Prod once its activities point at each workspace's items.

> **Which do I run to load data?**
> - Simplest demo → run the **notebooks** `NB_01`→`NB_03`, plus the **`DF_Gold_PA`**
>   dataflow before `NB_03` (it builds the Gold `production_daily` table).
> - One-click end-to-end refresh / post-deployment → run **`PL_Refresh_Master`** (it runs
>   all three notebooks **and** the `DF_Gold_PA` dataflow in the correct order).

## Quick Start

The demo is organized in three parts — do them in order, or stop after any part:

- **Part A — Build the data** (Steps 1–5): create the lakehouses and run the notebooks
  to populate the Bronze → Silver → Gold Medallion. Stop here for a pure data demo.
- **Part B — Add the analytics items** (Steps 6–9): import the data pipelines, build the
  dataflows, create the semantic model, and build the Power BI report.
- **Part C — Wire up CI/CD** (Steps 10–17): connect Git, create the Fabric Deployment
  Pipeline, configure GitHub Actions, and demo the promotion loop.

### Step 1 — Prerequisites
- Fabric capacity (F2+) provisioned and assigned to your workspace
- Two Fabric workspaces (this demo uses **`ws-CICD-Dev`** and **`ws-CICD-Prod`** —
  substitute your own names). Build everything in `ws-CICD-Dev`; `ws-CICD-Prod` is the
  promotion target
- For the CI/CD half: a GitHub repo with `develop` and `main` branches, and the
  Fabric admin switches **Git integration** and **GitHub integration** enabled
- For GitHub Actions automation: an **Entra service principal** with the Fabric admin
  switch *“Service principals can use Fabric APIs”* enabled, added as Admin on both
  workspaces and the deployment pipeline (full setup in Step 14)

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
4. `DF_Gold_PA` (Dataflow Gen2) — Silver → Gold **`production_daily`** (see Step 7)
5. `NB_03_Aggregate_Gold`     — Gold `cost_monthly` + `schedule_summary` + cross-domain
   `field_kpi_facts`. **Requires `DF_Gold_PA` to have run first** — its KPI cell joins
   `production_daily` and stops with a clear message if that table is missing.

> **Why the split?** The Gold layer is completed by **two** artifacts on purpose:
> the low-code **`DF_Gold_PA`** dataflow owns the production aggregation
> (`production_daily`), and **`NB_03`** owns the cost/schedule/KPI tables. Together
> they finish Gold — neither does the whole job alone. For a notebooks-only quick demo
> you still need to run `DF_Gold_PA` (Step 7) before `NB_03`.

After Step 5 you have a fully built Medallion lakehouse. Stop here for a data-only
demo, or continue to Part B to add the pipelines, semantic model, and report.

## Part B — Add the analytics items

### Step 6 — Import & run the data pipeline
The single Data Factory pipeline `PL_Refresh_Master` orchestrates the whole Medallion
build (pipeline type #1). This repo ships it in **two formats**:

| File | Use it for |
| --- | --- |
| `PL_Refresh_Master.json` | Git-integration item definition (the source of truth synced in Step 10) |
| `PL_Refresh_Master.zip` | **Fabric template** package for the pipeline editor's **Home → Import** button |

The `.zip` file is a real Fabric pipeline **template** (the same structure Fabric itself
produces via **Home → Export**: a `<Name>/` folder with `manifest.json` + an
ARM-style `<Name>.json`). Bring the pipeline in one of two ways:

- **Via Git (recommended):** the `.json` definition arrives automatically when you sync
  the repo in Step 10 — Fabric materializes it into a real pipeline item. No manual
  import needed.
- **Via template Import:** **New → Data pipeline → Home → Import from a template**, pick
  `pipelines/PL_Refresh_Master.zip`. The pipeline imports with all five activities and the
  correct ordering already wired up.

> 📌 **After importing, re-bind the activities to your items.** Fabric pipeline templates
> reference notebooks/dataflows/semantic models by **item GUID**, and a template can't know
> the GUIDs in *your* workspace, so they ship as placeholder GUIDs
> (`00000000-0000-0000-0000-000000000000`). Open each activity and pick the matching item:
> the three **Notebook** activities → `NB_01_Seed_Bronze`, `NB_02_Transform_Silver`,
> `NB_03_Aggregate_Gold`; the **Dataflow** activity → `DF_Gold_PA`; the **Semantic model
> refresh** activity → `Gold_SM`. `workspaceId` resolves automatically via
> `@pipeline().DataFactory`, so you don't set it by hand. If your tenant's **Import**
> rejects the template, fall back to **Git integration (Step 10)**, which always works for
> this exact definition.

Then open **`PL_Refresh_Master`** — it chains
`NB_01 → NB_02 → DF_Gold_PA → NB_03 → Refresh Gold_SM`, with each step gated on the
previous one. Leave `Environment = dev`. Run it once for a one-click end-to-end refresh.

> Bind the **Semantic model refresh** activity only after you create the semantic model in
> Step 8 — until then its item picker has nothing to point at.

### Step 7 — Build the Gold Dataflow Gen2 (`DF_Gold_PA`) — required
`dataflows/DF_Gold_PA.m` is the Power Query M script for the **one** dataflow in this
demo. It owns the Gold `production_daily` table — the single Gold table that
`NB_03_Aggregate_Gold` does **not** build — so the Gold layer is completed by **two
artifacts together**: `DF_Gold_PA` (production aggregation) + `NB_03` (cost / schedule /
KPI tables). This step is **not optional**: build and run `DF_Gold_PA` before `NB_03`
(or just run `PL_Refresh_Master`, which runs it for you as a Dataflow activity).

> ℹ️ **What is a Dataflow Gen2?** It's Fabric's low-code ETL item built on Power Query.
> You connect to a source, transform with the Power Query editor (or M code), and set a
> **data destination** (here, the lakehouse). The `DF_Gold_PA.m` file is the Power Query
> script behind the dataflow — you paste it into the editor below.

This repo ships the dataflow in **two formats** — pick whichever you prefer:

| File | Use it for |
| --- | --- |
| `dataflows/DF_Gold_PA.pqt` | **One-click import** — a real Power Query **template** (`Get data → Import from a Power Query template`) |
| `dataflows/DF_Gold_PA.m` | Manual paste into the Advanced editor (fallback / to read the logic) |

**Option A — Import the Power Query template (`.pqt`, fastest):**

1. In your workspace, **+ New item → Dataflow Gen2**. Name it **`DF_Gold_PA`**.
2. In the Power Query editor, **Get data → Import from a Power Query template** and select
   `dataflows/DF_Gold_PA.pqt`. Both queries (`SilverProduction`, `GoldProductionDaily`)
   load in already wired together.
3. **Point the source at *your* `Silver_LH`.** A Power Query template can't know the GUIDs
   of your lakehouse, so `SilverProduction` ships with two placeholder GUIDs. Open your
   `Silver_LH` in Fabric and copy the two IDs from its URL:
   `…/groups/<WORKSPACE_GUID>/lakehouses/<LAKEHOUSE_GUID>`. Then in the
   `SilverProduction` query's **Advanced editor**, replace the two
   `00000000-0000-0000-0000-000000000000` values with your `<WORKSPACE_GUID>` and
   `<LAKEHOUSE_GUID>`.
   - *Easiest alternative:* delete the `Source` step and re-create it with **Get data →
     More → Lakehouse → `Silver_LH` → `production_conformed`** — Fabric fills in the
     correct GUIDs for you.
   - This fixes the `Expression.Error: The key didn't match any rows in the table.` that
     appears while the placeholder GUIDs are still in place.
4. Set the **data destination** on **`GoldProductionDaily`** → **Lakehouse** → `Gold_LH` →
   table **`production_daily`**, **Update method = Replace**, then **Publish**.

**Option B — Paste the M manually:**

1. In your workspace, click **+ New item** → search **Dataflow Gen2** → select it (or use
   **New → More options → Data Factory → Dataflow Gen2**). Name it **`DF_Gold_PA`**.
2. The Power Query editor opens. You now need a query to paste the M into:
   - Click **Get data → Blank query** (under *Other* ), **or**
   - If you see the *Home* ribbon, click **Get data → Blank query**.
3. In the blank query, open the **Advanced editor**: *Home* ribbon → **Advanced editor**
   (or right-click the query in the left **Queries** pane → **Advanced editor**).
4. **Delete the default snippet**, then paste the body of `DF_Gold_PA.m`. ⚠️ Paste
   only the `let … in …` expression for each query — **not** the `section …;` line or the
   `shared QueryName =` prefix (those are file-packaging syntax, not valid in the editor).
   `DF_Gold_PA.m` defines two queries (`SilverProduction`, then `GoldProductionDaily`) —
   create one **Blank query** per query and paste each `let … in …` body separately.
5. Click **OK**. The `SilverProduction` source reads `Silver_LH.production_conformed`. The
   `.m` ships with two placeholder GUIDs — replace the `workspaceId` / `lakehouseId`
   `00000000-…` values with your `Silver_LH` IDs (from its URL
   `…/groups/<WORKSPACE_GUID>/lakehouses/<LAKEHOUSE_GUID>`), **or** simply re-create the
   `Source` step via **Get data → More → Lakehouse → `Silver_LH` → `production_conformed`**
   so Fabric fills in the correct GUIDs.
6. Set the **data destination** (bottom-right **Data destination** ⊕, or the gear on the
   last applied step) on the **`GoldProductionDaily`** query: choose **Lakehouse** →
   `Gold_LH` → table **`production_daily`**, **Update method = Replace**, map the columns,
   and confirm.
7. Click **Publish** (bottom-right). Fabric saves and runs the dataflow; refresh it any
   time from the workspace list (**⋯ → Refresh**) to rebuild `Gold_LH.production_daily`.

> 💡 Run `DF_Gold_PA` **before** `NB_03_Aggregate_Gold` — `NB_03`'s KPI cell joins
> `production_daily` and stops with a clear message if that table is missing.

### Step 8 — Create the semantic model (`Gold_SM`)
`semantic_model/Gold_SM.bim` is a TMSL model over the Gold layer using **DirectLake**.
It defines **4 tables** — `production_daily`, `cost_monthly`, `schedule_summary`, and a
`DateDim` date table — related on `date`, with **two headline measures**:
`Total BOE` (`SUM(production_daily[total_boe])`) and
`Total Cost (USD)` (`SUM(cost_monthly[total_cost_usd])`).

> **Where does `DateDim` come from?** It is **not** a Gold lakehouse table — there is no
> `Gold_LH.DateDim`. It's a **calculated table defined inside the semantic model** via a
> DAX `CALENDAR(DATE(2024,1,1), DATE(2024,12,31))` expression (see the `DateDim-calculated`
> partition in `Gold_SM.bim`), with calculated columns (`Year`, `Month`, `MonthName`,
> `Quarter`, `WeekNumber`). The model generates it at refresh time, so when you create the
> model from the `.bim` it comes along automatically. If you instead build the model by
> clicking **New semantic model** on `Gold_LH`, the lakehouse picker will **not** list a
> `DateDim` (it doesn't exist there) — add it afterward in the model as a calculated table
> with `DateDim = CALENDAR(DATE(2024,1,1), DATE(2024,12,31))`, then add the calculated
> columns. (Note: calculated tables/columns run in import/analysis mode alongside the
> DirectLake fact tables — a composite model — which is expected for this demo date table.)

Create it one of three ways:

- **From Fabric (fastest, click-through):**
  1. Open **`Gold_LH`** → top ribbon **New semantic model**.
  2. Name it **`Gold_SM`** and tick the Gold tables (`production_daily`, `cost_monthly`,
     `schedule_summary`). Click **Confirm** — this creates a DirectLake model.
     (`DateDim` is **not** in this list — you add it in the next step.)
  3. Add the date table: **New calculated table** →
     `DateDim = CALENDAR(DATE(2024,1,1), DATE(2024,12,31))`, then add calculated columns
     (`Year`, `Month`, `MonthName`, `Quarter`, `WeekNumber`) per `Gold_SM.bim`.
  4. **Model view** → drag `DateDim[Date]` onto `production_daily[date]` to create the
     relationship (single-direction, one-to-many from `DateDim`).
  5. Add the two measures: select `production_daily` → **New measure** →
     `Total BOE = SUM(production_daily[total_boe])`; select `cost_monthly` →
     **New measure** → `Total Cost (USD) = SUM(cost_monthly[total_cost_usd])`.

- **From the `.bim` (full fidelity, recommended for the demo):**
  1. Open **`Gold_SM.bim`** in **Tabular Editor 2/3** (or modern **Power BI Desktop**).
  2. Point the DirectLake source/expression at **this workspace's `Gold_LH` SQL
     analytics endpoint** (update the connection/`expressions` to your workspace + LH).
  3. **Save to / Deploy** to the workspace as **`Gold_SM`** (Tabular Editor:
     *Model → Deploy*; Power BI Desktop: **Publish**).

- **Via Git (hands-off):** the `.bim` + `definition.pbism` sync in with the repo in
  Step 10 and the model is created automatically — just rebind its DirectLake source to
  the target workspace's `Gold_LH` after the first sync.

> Publish from **modern Power BI Desktop / Tabular Editor** so the model has **Enhanced
> Metadata** — it's mandatory for Deployment Pipelines (see Limitations). Copy the
> model's **GUID** (workspace → model → *Settings*, or the item URL) and use it for the
> `SemanticModel_Gold` template parameter / `SemanticModelId` Deployment Rule so
> `PL_Refresh_Master`'s refresh activity can find it.

### Step 9 — Build the Power BI report (`Gold_Dashboard`)
With `Gold_SM` published, build the report on top of it:

1. **Create the report (live, DirectLake):**
   - In the workspace: **New → Report → pick a published semantic model → `Gold_SM`**, or
   - In **Power BI Desktop**: **Get data → Power BI semantic models → `Gold_SM`** in
     **Live connect** mode — never *Import*, so the report stays DirectLake.
2. **Add visuals over the two measures**, for example:
   - **Card** — `Total BOE` (production at a glance).
   - **Card** — `Total Cost (USD)` (spend at a glance).
   - **Line chart** — `Total BOE` by `DateDim[Date]` (production trend).
   - **Clustered column** — `Total BOE` and `Total Cost (USD)` by `field` (compare fields).
   - **Combo chart** — `Total Cost (USD)` (columns) vs `Total BOE` (line) by
     `DateDim[MonthName]` (cost efficiency over time).
3. **Save / publish** it to the workspace as **`Gold_Dashboard`**.

> Keep the report in **Live connect / DirectLake** (no imported tables) so it promotes
> cleanly through the Deployment Pipeline and always reflects the latest Gold data after
> `PL_Refresh_Master` runs.

After Part B the workspace holds every item the Deployment Pipeline will promote:
lakehouses, notebooks, data pipelines, dataflows, the semantic model, and the report.

## Part C — Wire up CI/CD

### Step 10 — Connect Git Integration
In the workspace: **Workspace settings → Git integration → GitHub** — select the repo,
branch, and folder, then connect. **Source control → Update** pulls every item
(notebooks, pipelines, dataflows, semantic model) into the workspace.

### Step 11 — Commit to GitHub
In the workspace: **Source control** icon → select all items → add a commit message → **Commit**

### Step 12 — Create the Deployment Pipeline
In the Fabric portal: **Workspaces → Deployment pipelines → New pipeline**. Create two
stages (**Development → Production**) and assign `ws-CICD-Dev` to Development and
`ws-CICD-Prod` to Production. This **Fabric Deployment Pipeline** (pipeline type #2)
promotes items across Dev → Prod.

1. Open the pipeline → the **Development** stage shows all items from `ws-CICD-Dev`.
2. Click **Deploy** once (Dev → Prod) to create the paired items in `ws-CICD-Prod`.
   Verify in the **compare view** that every item is *paired* (linked icon), not
   duplicated — unpaired same-name items create copies (see Limitations).
3. Copy the **pipeline ID** from the browser URL (the GUID after `/pipelines/`). You'll
   add it to GitHub secrets as `FABRIC_PIPELINE_ID` in Step 14.

### Step 13 — Configure the Deployment Rules
**Deployment Rules** make the *same* item behave correctly in each stage — e.g. point
the semantic model at the Prod `Gold_LH`, or pass `Environment=prod` to the pipeline.
They are configured **once in the portal**, stored **on the deployment pipeline**, and
applied **automatically on every deploy** (including deploys triggered by GitHub
Actions — there is nothing rule-related to put in the YAML).

`deployment_rules/deployment_rules.json` is the **reference** for what to enter (the
rules themselves can't be imported from a file — see the automation note below). In the
pipeline, click the **⚙️ Deployment rules** icon on the **Production** stage and add one
rule per item:

| Item | Rule type | What to set | Source in JSON |
|------|-----------|-------------|----------------|
| `PL_Refresh_Master` | **Parameter rule** | `Environment` → `prod` | rule 1 |
| `PL_Refresh_Master` | **Parameter rule** | `SemanticModelId` → the Prod `Gold_SM` GUID | rule 2 |
| `Gold_SM` | **Data source rule** | Lakehouse binding → `ws-CICD-Prod/Gold_LH` SQL endpoint | rule 3 |

> **Where do the IDs come from?** The `Gold_SM` GUID comes from Step 8; connection IDs
> come from **Fabric Admin portal → Connections**. The JSON uses placeholders like
> `<PROD_GOLD_SM_GUID>` — replace them with your real values as you fill in the UI.

Click **Save**. From now on, every Dev → Prod deploy rewrites these settings in the
target automatically.

> **💡 Can the rules be automated?** Not really — deployment **rules** are effectively
> portal-only (no stable public REST API to create them), so a notebook/script can't
> reliably set them. What *can* be automated (and is, via GitHub Actions in the next
> steps): creating the pipeline, assigning workspaces, triggering the deploy, and
> running the post-deploy refresh. For richer cross-stage parameterization, Microsoft's
> **`fabric-cicd`** Python library + a `parameter.yml` is the supported alternative.

### Step 14 — Create a service principal & add GitHub secrets
GitHub Actions deploys by calling the Fabric REST API as a **service principal** (SP),
so it needs its own identity and secrets:

1. **Register an Entra app** (Azure portal → **App registrations → New registration**).
   Note the **Application (client) ID** and **Directory (tenant) ID**; under
   **Certificates & secrets**, create a **client secret** and copy its value.
2. **Enable the Fabric admin switch** *“Service principals can use Fabric APIs”*
   (Fabric Admin portal → Tenant settings), scoped to a security group that contains
   the SP.
3. **Grant the SP access:** add it as a **Member/Admin** on both `ws-CICD-Dev` and
   `ws-CICD-Prod`, and as an **Admin on the deployment pipeline** (Manage access).
4. **Add the four secrets** in GitHub (**Settings → Secrets and variables → Actions**):
   - `FABRIC_TENANT_ID` — Directory (tenant) ID
   - `FABRIC_CLIENT_ID` — Application (client) ID
   - `FABRIC_CLIENT_SECRET` — the client secret value
   - `FABRIC_PIPELINE_ID` — the pipeline GUID from Step 12

### Step 15 — Add the GitHub Actions workflow
Copy `github_actions/deploy-dev-to-prod.yml` to `.github/workflows/` in your repo.
What it does and how it ties together:

- **Trigger:** runs on merge/push to `main` (or manual **Run workflow**).
- **Auth:** exchanges the three SP secrets for a Fabric bearer token.
- **Deploy:** calls `POST /v1/pipelines/{FABRIC_PIPELINE_ID}/deploy` with
  `sourceStageOrder: 0` (Dev) → `targetStageOrder: 1` (Prod). The **deployment rules**
  from Step 13 are applied **server-side** during this call — the YAML never references
  them.
- **Gate:** the job uses the GitHub **`production` environment**, so it pauses for
  reviewer approval before the promotion runs. Configure that under
  **Settings → Environments → production → Required reviewers**.
- **Wait & report:** it polls the operation status and writes a deployment summary.

### Step 16 — Refresh data after deployment
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

### Step 17 — Demo the CI/CD Loop
1. Open `NB_02_Transform_Silver` in your dev workspace (`ws-CICD-Dev`)
2. Uncomment the `gor_ratio` line (Gas-to-Oil Ratio KPI)
3. Run the notebook — verify the new column appears
4. Source Control → Commit: `feat: add gas-to-oil ratio KPI`
5. GitHub: open PR from develop → main → review the diff → merge
6. GitHub Actions runs `deploy-dev-to-prod.yml` → waits for reviewer approval on the
   `production` environment, then promotes Dev → Prod (deployment rules re-bind the
   Prod semantic model and parameters automatically)

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
