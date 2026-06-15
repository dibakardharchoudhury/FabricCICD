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
| 3 Lakehouses | Deployment Pipeline | Paired by name — arrives **empty (no tables, no data)**; notebooks recreate tables on run (Step 13) | — |
| 4 Notebooks | Deployment Pipeline | **Default-lakehouse rule** → Prod LH (code also uses name-based three-part names) | ✅ rule |
| Data pipeline `PL_Refresh_Master` | Deployment Pipeline | Activity GUIDs **auto-paired/rewritten** | — |
| Dataflow `DF_Gold_PA` | Deployment Pipeline | **Parameter rule** → Prod `Silver_LH` IDs | ✅ rule |
| Semantic model `Gold_SM` (Direct Lake on OneLake) | Deployment Pipeline | **Parameter rule (Text)** → Prod `Gold_LH` IDs — Direct Lake **on OneLake can't use a data-source rule**; never auto-binds; **you must own the model** | ✅ rule + own |
| Report `Gold_Dashboard` | Deployment Pipeline | **Autobinds** to the paired Prod `Gold_SM` | — |

### Binding options — simplest first

**Option 1 — Deployment rules only (recommended).** Three rules on the Production stage cover everything that doesn't auto-bind (notebook default lakehouse, dataflow parameters, semantic-model connection parameter). No extra items, no code changes. You create them **once** (after the first deploy, since a rule can only attach to an item that already exists in Prod) and they re-apply on every deploy after that. This is Steps 11–13. `deployment_rules/deployment_rules.json` lists the exact values.

**Option 2 — Variable Library + the semantic-model rule.** A Variable Library (`VL_CICD_Bindings`) with a value set per stage can drive the two **data-plane** items at runtime — [supported consumers](https://learn.microsoft.com/en-us/fabric/cicd/variable-library/variable-library-overview#supported-items) include **notebooks** (an item-reference variable sets the default lakehouse via `%%configure`/NotebookUtils) and **Dataflow Gen2** (binds `SilverWorkspaceId`/`SilverLakehouseId`). This replaces the notebook + dataflow rules with config-as-code. **But the semantic model is *not* a Variable Library consumer** — so `Gold_SM` *always* needs its own **parameter rule** (and you must own the model). Choose Option 2 when you want config-as-code or many items share the same IDs. See `variable_library/VL_CICD_Bindings.json`.

### Part C1 — Manual promotion

**10. Connect Git & commit (Dev)** — Workspace settings → Git integration → GitHub → connect → **Update**, then Source control → select items → **Commit**.

**11. Create the pipeline & do the FIRST deploy** — Deployment pipelines → New → two stages (**Development → Production**); assign `ws-CICD-Dev` to Development and the empty `ws-CICD-Prod` to Production. Select **all** items in Development → **Deploy** (one batch — separate batches don't auto-pair and create duplicates). Copy the **pipeline GUID** from the URL (needed for automation).

> **Why deploy before creating rules?** A Fabric deployment rule can only attach to an item that **already exists in the target stage**. Until this first deploy runs, `ws-CICD-Prod` is empty and the **Deployment rules** panel has nothing to configure. So the order is always: **deploy once → set rules → deploy again**. After this first deploy the Prod items exist but a few still point back at Dev — Step 12 fixes that, Step 13 re-applies it.

**12. Create the three deployment rules (Production stage)** — in the pipeline, click the **Production** stage. Items that support a rule show a **⚙️ / lightning Deployment rules** icon — click it on each item below, add the rule, **Save**. Only these three need one:

| Artifact | Rule type to add | Set the Production value to |
| --- | --- | --- |
| `NB_01`, `NB_02`, `NB_03` (one rule each) | **Default lakehouse** | That notebook's Prod lakehouse in `ws-CICD-Prod` (`NB_01`→`Bronze_LH`, `NB_02`→`Silver_LH`, `NB_03`→`Gold_LH`) |
| `DF_Gold_PA` | **Parameter** | `SilverWorkspaceId` = `ws-CICD-Prod` ID · `SilverLakehouseId` = Prod `Silver_LH` ID |
| `Gold_SM` (Direct Lake on OneLake) | **Parameter** (Text) — *take ownership first* | `WorkspaceId` = `ws-CICD-Prod` ID · `LakehouseId` = Prod `Gold_LH` ID (the model's connection must already be parameterized — see callout) |

  **No rule needed:** `PL_Refresh_Master` (activity GUIDs auto-pair) and `Gold_Dashboard` (autobinds to the paired Prod `Gold_SM`). Lakehouses just pair by name. Exact values are in `deployment_rules/deployment_rules.json`. Rules are portal-only (no REST API) and, once set, re-apply automatically on **every** future deploy — you set them once.

> **`Gold_SM` rules greyed out? (Direct Lake on OneLake)** Two things gate the rule pane, and both apply here:
>
> 1. **You must be the *owner* of the model** (docs: *"View or set a rule — Owner of the item you're setting a rule for"*). The first deploy clones a **new** `Gold_SM` in Prod and *"the user who made the deployment becomes the owner"* — if a different identity (or service principal) deployed it, every rule section greys out.
>    - **Take-over steps:** in `ws-CICD-Prod`, open `Gold_SM` → **… (More options) → Settings** (or the **Take over** banner) → **Take over / Take ownership** → confirm. Reopen the pipeline's **Production** stage → `Gold_SM` → **Deployment rules** — the pane is now editable.
> 2. **A *data-source* rule is not supported for Direct Lake on OneLake.** Per the [Direct Lake limitations](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview#considerations-and-limitations), *"Deployment pipeline rules to rebind data source — Not supported directly; can create a parameter expression to use in the connection string."* So even as owner, the **Data source** option stays empty. Instead rebind with a **Parameter rule (Text)**:
>    - In the model's connection (TMDL / XMLA or web-modeling connection string), replace the hard-coded Dev workspace/lakehouse GUIDs with **Text parameters** (e.g. `WorkspaceId`, `LakehouseId`).
>    - Deploy once so the parameterized model exists in Prod, take ownership, then in the Production stage add a **Parameter** rule on `Gold_SM` setting `WorkspaceId`/`LakehouseId` to the Prod values. Parameter rules **must be type `Text`**.
>
> Direct Lake **never auto-binds** and Dataflow Gen2 **never auto-binds**, so without these rules a re-deploy leaves `Gold_SM` and `DF_Gold_PA` pointing at **Dev**.

  *Option 2 (Variable Library):* instead of the notebook + dataflow rules, point those items at `VL_CICD_Bindings` and set the active value set per stage; keep only the `Gold_SM` parameter rule (the semantic model is not a Variable Library consumer).

**13. Deploy again, reload data & verify** — click **Deploy** once more so the rules apply and the Prod items repoint to Prod sources. The Prod lakehouses arrive **completely empty** (see callout), so then:
1. **Pairing** — every item shows the chain-link (paired) icon; none "only in source"; no duplicate same-name items.
2. **Load** — run `PL_Refresh_Master` once in Prod (`NB_Setup → NB_01 → NB_02 → DF_Gold_PA → NB_03`). This **creates the tables and loads the data** in one pass.
3. **Semantic model** — `Gold_SM` lineage points at **Prod** `Gold_LH`; refresh; `Total BOE` / `Total Cost (USD)` return values.
4. **Report** — open `Gold_Dashboard` in Prod; it's bound to the Prod `Gold_SM` and renders Prod data.

> **Why is the Prod lakehouse empty — wasn't the table schema in Git?** No. Per the [official docs](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-git-deployment-pipelines#what-is-tracked), Git tracks only the lakehouse **metadata/container** (name, GUID, SQL-endpoint metadata, `shortcuts.metadata.json`). **Tables (Delta and non-Delta) are explicitly "not tracked / not supported"** — schema *and* data live in OneLake, not Git. The deployment pipeline's documented default is *"a new **empty** lakehouse with the same name is created in the target workspace."* So tables don't exist in Prod until something recreates them.
>
> **The pipeline promotes code & artifacts — never data.** Each environment establishes its own tables. Pick the pattern that fits (this demo uses #1):
> 1. **DDL-in-code (this repo).** Notebooks/pipelines carry the `CREATE TABLE` logic and *do* promote. Run `PL_Refresh_Master` in Prod (13.2) → schema **and** data in one pass. Best when Prod ingests from its own source.
> 2. **Shortcuts, not copies.** Point Prod tables at a shared source via a OneLake internal shortcut (auto-remapped across stages) or an external ADLS/S3 shortcut — no data copy, and the shortcut *definition* is tracked in Git.
> 3. **Explicit data copy.** A Copy-activity pipeline or notebook clones Dev→Prod tables (`DEEP CLONE`, or `notebookutils.fs.cp` on the Delta folders via OneLake paths) when Prod must mirror Dev exactly.
> 4. **Prod re-ingests from the production source.** The enterprise norm: Dev loads sample data, Prod loads real data from the upstream system. Empty-on-deploy is by design.

### Part C2 — Automate the promotion

**14. Service principal & secrets** — register an Entra app (client/tenant ID, secret); enable *"Service principals can use Fabric APIs"*; add it as Admin on both workspaces and the pipeline. GitHub secrets: `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_PIPELINE_ID` (the pipeline GUID from Step 11).

**15. Add the workflow** — copy `github_actions/deploy-dev-to-prod.yml` to `.github/workflows/`. On push to `main` it acquires a Fabric token, calls `POST /v1/pipelines/{id}/deploy` (Dev→Prod), and gates on the GitHub `production` environment (add required reviewers). The deployment rules apply server-side, so the only post-deploy step is the data reload (Step 13.2).

**16. Demo the loop** — edit `NB_02` (e.g. uncomment the `gor_ratio` KPI) → run → Commit → PR to `main` → merge → Actions promotes Dev→Prod after reviewer approval.

---

## Key limitations

| Area | Limitation | Mitigation |
| --- | --- | --- |
| Git | Sensitivity labels block commits; 50 MB/commit; Admin-only connect; no MyWorkspace | Remove labels; batch commits; pre-configure; use named workspaces |
| Git | Lakehouse Git/deploy tracks only **metadata** — Tables (Delta & non-Delta) are *"not tracked / not supported"* (per docs) | Recreate schema via notebooks/pipeline, shortcut a shared source, copy tables explicitly, or re-ingest from the prod source (Step 13 callout) |
| Deploy | Lakehouse arrives **empty** — docs: *"a new empty lakehouse… is created in the target workspace"* | Run `PL_Refresh_Master` after deploy (creates schema + loads data) |
| Deploy | Items added after assignment don't auto-pair; same-name unpaired items duplicate | Build all items first; verify pairing before deploying |
| Deploy | Direct Lake on OneLake semantic models don't auto-bind | Parameterize the connection, then a **Text parameter rule** → Prod `Gold_LH` IDs (Step 12) |
| Deploy | Direct Lake **on OneLake** can't use a **data-source** rule (docs) | Use a connection-string **parameter** + parameter rule instead |
| Deploy | Semantic-model rules require **item ownership** — greyed out otherwise | **Take over** `Gold_SM` in the Prod workspace, then set the rule |
| Deploy | Dataflow Gen2 doesn't auto-bind | Parameter rule → Prod `Silver_LH` (Step 12), or Variable Library |
| Deploy | Semantic model is **not** a Variable Library consumer | `Gold_SM` always needs its own parameter rule, even with Option 2 |
| Notebook | Default-lakehouse GUID is workspace-specific | Default-lakehouse rule (Step 12); code also uses three-part names |
| Semantic model | Needs Enhanced Metadata for pipelines | Fabric **New semantic model** already has it |
