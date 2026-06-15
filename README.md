# Microsoft Fabric CI/CD Demo — Medallion Architecture

End-to-end CI/CD with a Medallion Lakehouse, GitHub integration, and Fabric Deployment Pipelines.

## Architecture

```
Sources (PIMS / Alpha / SharePoint)
  └─► Bronze_LH   ← NB_01_Seed_Bronze
        └─► Silver_LH ← NB_02_Transform_Silver
              └─► Gold_LH ← DF_Gold_PA (production_daily) + NB_03_Aggregate_Gold (cost / schedule / KPI)
                    └─► Gold_SM (DirectLake semantic model) → Gold_Dashboard (report)

Pipeline order: NB_Setup → NB_Seed_Bronze → NB_Transform_Silver → DF_Gold_PA → NB_Aggregate_Gold
CI/CD:  GitHub (main) ─► GitHub Actions ─► Fabric Deployment Pipeline (ws-CICD-Dev → ws-CICD-Prod)
```

## Repo layout

| Path | What it is |
| --- | --- |
| `NB_0*.Notebook/` | Fabric Git notebook items (`.platform` + `notebook-content.py`) for Bronze→Silver→Gold |
| `*_LH.Lakehouse/` | Fabric Git lakehouse items (Bronze / Silver / Gold) |
| `dataflows/DF_Gold_PA.{pqt,m}` | Gold `production_daily` Dataflow Gen2 — `.pqt` template + `.m` source |
| `pipelines/PL_Refresh_Master.zip` | End-to-end orchestration — one-click **import template** (a verified Fabric export) |
| `variable_library/VL_CICD_Bindings.json` | Reference for the Variable Library (per-stage lakehouse/workspace IDs) |
| `deployment_rules/deployment_rules.json` | Reference for the per-stage Deployment Rules |
| `github_actions/deploy-dev-to-prod.yml` | CD workflow: promotes Dev → Prod via the Fabric REST API |
| `sample_data/*.csv` | Seed data for the Bronze layer |

## "Pipeline" means three different things here

| # | Name | Lives in | Does |
| --- | --- | --- | --- |
| 1 | **Data pipeline** | `pipelines/PL_Refresh_Master` | Runs the whole Medallion build inside a workspace |
| 2 | **Deployment pipeline** | Fabric portal | Promotes items across Dev → Prod |
| 3 | **GitHub Actions** | `github_actions/*.yml` | Triggers the deployment pipeline on push to `main` |

---

## Part A — Build the data

**1. Prerequisites** — Fabric capacity (F2+); two workspaces (`ws-CICD-Dev`, `ws-CICD-Prod`); Git/GitHub integration enabled. Automation also needs an Entra service principal (Step 14).

**2. Create three lakehouses** in `ws-CICD-Dev` with these **exact** names: `Bronze_LH`, `Silver_LH`, `Gold_LH`. Notebooks reference them by three-part name (e.g. `Bronze_LH.dbo.production_raw`), so names must match — this is also what keeps them portable across workspaces (resolved by name, not GUID).

**3. Sync notebooks from Git** — Workspace settings → Git integration → connect this repo → Source control → **Update**. The `NB_*.Notebook/` items arrive as real notebooks.

**4. Attach lakehouses + set default** ⚠️ — open `NB_01`/`NB_02`/`NB_03` → **Explorer → Lakehouses** → add all three and set one default. The default-lakehouse GUID is workspace-specific (not in Git), so set it once in Dev; promotion rebinds it in Prod automatically (Step 12). Without an attached lakehouse, writes fail with `[SCHEMA_NOT_FOUND]`. `NB_00` only verifies the lakehouses exist.

**5. Run in order:** `NB_00` → `NB_01` → `NB_02` → **`DF_Gold_PA`** (Step 7) → `NB_03`. Gold is built by two items: `DF_Gold_PA` → `production_daily`; `NB_03` → `cost_monthly` + `schedule_summary` + `field_kpi_facts`. `NB_03` joins `production_daily`, so run the Dataflow first.

---

## Part B — Add the analytics items

**6. Import & run the data pipeline `PL_Refresh_Master`** — New → Data pipeline → **Home → Import from a template** → `pipelines/PL_Refresh_Master.zip`. Five activities import pre-wired: `NB_Setup → NB_01 → NB_02 → DF_Gold_PA → NB_03`, each gated on the previous. After import, **open each activity and re-pick your own item** (shipped GUIDs point at the author's workspace). Git sync and Deployment-Pipeline promotion re-pair these automatically — manual re-pick is only for the first template import.

  > After you commit from Fabric (Step 10), the pipeline serializes to `PL_Refresh_Master.DataPipeline/` in Git — that becomes the source of truth; the `.zip` is import-only.

**7. Build the Gold Dataflow `DF_Gold_PA`** (builds `production_daily`):
1. **+ New item → Dataflow Gen2**, name `DF_Gold_PA`.
2. **Get data → Import from a Power Query template** → `dataflows/DF_Gold_PA.pqt` (loads parameters `SilverWorkspaceId`/`SilverLakehouseId` and queries `SilverProduction`/`GoldProductionDaily`).
3. Set the two parameters to **your `Silver_LH` IDs** from its URL `…/groups/<SilverWorkspaceId>/lakehouses/<SilverLakehouseId>`.
4. On `GoldProductionDaily` set **data destination → Lakehouse → `Gold_LH` → `production_daily`**, Update method **Replace**, then **Publish**.

   (Fallback: paste each `let … in …` body from `DF_Gold_PA.m` into a Blank query, omitting the `section`/`shared` lines.)

**8. Create the semantic model `Gold_SM`** — open `Gold_LH` → **New semantic model** → tick the four Gold tables (`production_daily`, `cost_monthly`, `schedule_summary`, `field_kpi_facts`) → Confirm. Publishing gives it **Enhanced Metadata** (required for Deployment Pipelines). After you commit from Fabric (Step 10) it serializes to `Gold_SM.SemanticModel/` (TMDL) in Git. Add the date dimension, relationships and measures (a–c):

  **a. Calculated table `DateDim`** — Model view → **New table**:
  ```DAX
  DateDim = CALENDAR(DATE(2024,1,1), DATE(2024,12,31))
  ```
  Then add these **calculated columns** (one **New column** each, on `DateDim`):
  ```DAX
  Year       = YEAR([Date])
  Month      = MONTH([Date])
  MonthName  = FORMAT([Date], "MMM YYYY")
  Quarter    = "Q" & QUARTER([Date])
  WeekNumber = WEEKNUM([Date])
  ```

  **b. Relationships** — exactly **two**:
  - `production_daily[date]` → `DateDim[Date]` (many-to-one, single, **active**) — drives time slicing.
  - `production_daily[field]` → `field_kpi_facts[field]` (many-to-one, single, **inactive**) — for `USERELATIONSHIP`.

  `cost_monthly` and `schedule_summary` are intentionally unrelated — they're pre-aggregated at different grains; each visual slices them by its own `field`/`cost_type`/`priority` columns.

  **c. Measures** — right-click the table → **New measure**, paste, set format:

  On **`production_daily`**:
  ```DAX
  Total BOE    = SUM(production_daily[total_boe])                                               // format: #,##0.00
  BOE Last Day = CALCULATE(SUM(production_daily[total_boe]), LASTDATE(production_daily[date]))   // format: #,##0.00
  ```
  On **`cost_monthly`**:
  ```DAX
  Total Cost (USD) = SUM(cost_monthly[total_cost_usd])                                          // format: $ #,##0
  OPEX Total       = CALCULATE(SUM(cost_monthly[total_cost_usd]), cost_monthly[cost_type] = "OPEX")    // $ #,##0
  CAPEX Total      = CALCULATE(SUM(cost_monthly[total_cost_usd]), cost_monthly[cost_type] = "CAPEX")   // $ #,##0
  ```
  **Save.**

> ⚠️ **`production_daily` missing from the table picker?** Dataflow Gen2 tables appear in the SQL endpoint / OneLake picker only **after a metadata sync**. Open **`Gold_LH` → SQL analytics endpoint → Refresh**, wait a few seconds, and re-open the dialog.

**9. Build the report `Gold_Dashboard` with Copilot** — New → Report → live-connect to `Gold_SM` (DirectLake, never Import). Open the **Copilot** pane and prompt it, e.g.:

  > "Create a Production Analysis page with cards for Total BOE, Total Cost (USD) and BOE Last Day; a column chart of Total BOE by field; a line chart of Total BOE over DateDim[Date]; and a clustered column chart of Total Cost (USD) by field broken out by cost_type."

  Review, tweak, **Save as `Gold_Dashboard`** in the workspace. It promotes through the deployment pipeline and **autobinds** to the paired Prod `Gold_SM` (Step 12) — no rule, no Git step. Commit it from Fabric (Step 10) for source control like every other item.

---

## Part C — CI/CD

Promotion model: **Git** is source control into Dev; the **Deployment Pipeline** promotes every item Dev → Prod. Part C1 is the manual promotion; Part C2 automates it.

### How each item reaches a fresh Prod workspace without orphaned bindings

Deploy **all items in one pass** so the pipeline pairs them by name and rewrites internal references to the Prod copies. Only **three** bindings don't auto-rebind — each gets one deployment rule; everything else is automatic.

| Item | Promotes via | Rebinds to Prod by | Rule? |
| --- | --- | --- | --- |
| 3 Lakehouses | Deployment Pipeline | Paired by name (structure only — **reload data**, Step 13) | — |
| 4 Notebooks | Deployment Pipeline | **Default-lakehouse rule** → Prod LH (code also uses name-based three-part names) | ✅ rule |
| Data pipeline `PL_Refresh_Master` | Deployment Pipeline | Activity GUIDs **auto-paired/rewritten** | — |
| Dataflow `DF_Gold_PA` | Deployment Pipeline | **Parameter rule** → Prod `Silver_LH` IDs | ✅ rule |
| Semantic model `Gold_SM` (DirectLake) | Deployment Pipeline | **Data source rule** → Prod `Gold_LH` (DirectLake never auto-binds) | ✅ rule |
| Report `Gold_Dashboard` | Deployment Pipeline | **Autobinds** to the paired Prod `Gold_SM` | — |

### Binding options — simplest first

**Option 1 — Deployment rules only (recommended).** Three rules on the Production stage cover everything that doesn't auto-bind (notebook default lakehouse, dataflow parameters, semantic-model data source). No extra items, no code changes. Set once; they re-apply on every deploy. This is Step 12. `deployment_rules/deployment_rules.json` lists the exact values.

**Option 2 — Variable Library + the semantic-model rule.** Move the notebook and dataflow IDs into a Variable Library (`VL_CICD_Bindings`) with a value set per stage, so they resolve **at runtime** with no GUID in code; keep the `Gold_SM` data-source rule (DirectLake can't consume a Variable Library). Choose this only when you want config-as-code or many items share the same IDs. See `variable_library/VL_CICD_Bindings.json`.

### Part C1 — Manual promotion

**10. Connect Git & commit (Dev)** — Workspace settings → Git integration → GitHub → connect → **Update**, then Source control → select items → **Commit**.

**11. Create the Deployment Pipeline** — Deployment pipelines → New → two stages (**Development → Production**); assign `ws-CICD-Dev` to Development and empty `ws-CICD-Prod` to Production. Select **all** items in Development and **Deploy once** — separate batches don't auto-pair and create duplicates. Copy the **pipeline GUID** from the URL (needed for automation).

**12. Set the three Production-stage deployment rules** — on the **Production** stage click **⚙️ Deployment rules** and add:
- **`NB_01`/`NB_02`/`NB_03` → Default lakehouse** → map each Dev lakehouse to its matching Prod lakehouse.
- **`DF_Gold_PA` → Parameters** → `SilverWorkspaceId`/`SilverLakehouseId` = Prod `Silver_LH`.
- **`Gold_SM` → Data source** → `ws-CICD-Prod/Gold_LH` (DirectLake never auto-binds).

  That's all. `PL_Refresh_Master`'s activity GUIDs auto-pair and the report autobinds to the paired Prod `Gold_SM` — neither needs a rule. Rules are portal-only (no REST API) and re-apply on every deploy.

  *Option 2 (Variable Library):* instead of the notebook + dataflow rules, point those items at `VL_CICD_Bindings` and set the active value set per stage; keep only the `Gold_SM` data-source rule.

**13. Reload data & verify** — lakehouse deploys carry **structure only, no data**:
1. **Pairing** — every item shows the chain-link (paired) icon; none "only in source"; no duplicate same-name items.
2. **Load** — run `PL_Refresh_Master` once in Prod (`NB_Setup → NB_01 → NB_02 → DF_Gold_PA → NB_03`).
3. **Semantic model** — `Gold_SM` lineage points at **Prod** `Gold_LH`; refresh; `Total BOE` / `Total Cost (USD)` return values.
4. **Report** — open `Gold_Dashboard` in Prod; it's bound to the Prod `Gold_SM` and renders Prod data.

### Part C2 — Automate the promotion

**14. Service principal & secrets** — register an Entra app (client/tenant ID, secret); enable *"Service principals can use Fabric APIs"*; add it as Admin on both workspaces and the pipeline. GitHub secrets: `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_PIPELINE_ID` (the pipeline GUID from Step 11).

**15. Add the workflow** — copy `github_actions/deploy-dev-to-prod.yml` to `.github/workflows/`. On push to `main` it acquires a Fabric token, calls `POST /v1/pipelines/{id}/deploy` (Dev→Prod), and gates on the GitHub `production` environment (add required reviewers). The deployment rules apply server-side, so the only post-deploy step is the data reload (Step 13.2).

**16. Demo the loop** — edit `NB_02` (e.g. uncomment the `gor_ratio` KPI) → run → Commit → PR to `main` → merge → Actions promotes Dev→Prod after reviewer approval.

---

## Key limitations

| Area | Limitation | Mitigation |
| --- | --- | --- |
| Git | Sensitivity labels block commits; 50 MB/commit; Admin-only connect; no MyWorkspace | Remove labels; batch commits; pre-configure; use named workspaces |
| Deploy | Lakehouse copies structure, not data | Run `PL_Refresh_Master` after each deploy |
| Deploy | Items added after assignment don't auto-pair; same-name unpaired items duplicate | Build all items first; verify pairing before deploying |
| Deploy | DirectLake semantic models don't auto-bind | Data-source rule → Prod `Gold_LH` (Step 12) |
| Deploy | Dataflow Gen2 doesn't auto-bind | Parameter rule → Prod `Silver_LH` (Step 12) |
| Notebook | Default-lakehouse GUID is workspace-specific | Default-lakehouse rule (Step 12); code also uses three-part names |
| Semantic model | Needs Enhanced Metadata for pipelines | Fabric **New semantic model** already has it |
