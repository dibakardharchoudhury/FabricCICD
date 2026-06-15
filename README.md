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
| `pipelines/PL_Refresh_Master.{json,zip}` | End-to-end orchestration — `.json` Git source + `.zip` import template |
| `semantic_model/Gold_SM.bim` + `definition.pbism` | DirectLake TMSL model over the Gold layer |
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

**1. Prerequisites** — Fabric capacity (F2+); two workspaces (`ws-CICD-Dev`, `ws-CICD-Prod`); Git + GitHub integration enabled. For automation: an Entra service principal allowed to use Fabric APIs (full setup in Step 13).

**2. Create three lakehouses** in `ws-CICD-Dev` with these **exact** names: `Bronze_LH`, `Silver_LH`, `Gold_LH`. Notebooks write three-part names (e.g. `Bronze_LH.dbo.production_raw`), so names must match.

**3. Sync notebooks from Git** — Workspace settings → Git integration → connect this repo → Source control → **Update**. The `NB_*.Notebook/` items arrive as real notebooks; no manual `.py` import.

**4. Attach lakehouses to each notebook** ⚠️ — open `NB_01`/`NB_02`/`NB_03`, in **Explorer → Lakehouses** add all three lakehouses and set one default. Attached-lakehouse GUIDs are workspace-specific and **not** committed to Git, so re-attach once per workspace after every sync/deploy. Without this, cross-lakehouse writes fail with `[SCHEMA_NOT_FOUND]`. `NB_00` only verifies the lakehouses exist.

**5. Run in order:** `NB_00` → `NB_01` → `NB_02` → **`DF_Gold_PA`** (Step 7) → `NB_03`.
Gold is finished by **two** artifacts: `DF_Gold_PA` builds `production_daily`; `NB_03` builds `cost_monthly` + `schedule_summary` + `field_kpi_facts`. `NB_03` joins `production_daily`, so run `DF_Gold_PA` first.

---

## Part B — Add the analytics items

**6. Import & run the data pipeline `PL_Refresh_Master`** — two ways:
- **Via Git (recommended):** the `.json` arrives when you sync in Step 10; Fabric materializes the pipeline item.
- **Via template:** New → Data pipeline → **Home → Import from a template** → `pipelines/PL_Refresh_Master.zip` (this is a verified Fabric export and imports + saves cleanly). The five activities import pre-wired.

  Fabric pipelines reference items by **literal GUID** (that's what Fabric exports — expression-based `notebookId`/`workspaceId` makes the importer hang). The shipped GUIDs point at the author's workspace, so after a template import into **your** workspace **re-bind each activity**: the four Notebook activities → `NB_Setup`/`NB_01`/`NB_02`/`NB_03`; the Dataflow activity → `DF_Gold_PA`. If you sync via Git into the same workspace, or promote through the Deployment Pipeline, the GUIDs are re-paired automatically (Step 11) — no manual rebind.

  Run order: `NB_Setup → NB_01 → NB_02 → DF_Gold_PA → NB_03`, each gated on the previous. One run = full end-to-end Gold build.

**7. Build the Gold Dataflow `DF_Gold_PA`** (required — builds `production_daily`):
1. **+ New item → Dataflow Gen2**, name it `DF_Gold_PA`.
2. **Get data → Import from a Power Query template** → `dataflows/DF_Gold_PA.pqt`. It loads two parameters (`SilverWorkspaceId`, `SilverLakehouseId`) and two queries (`SilverProduction`, `GoldProductionDaily`).
3. Set the two parameters to **your `Silver_LH` IDs** (the read source) from its URL `…/groups/<SilverWorkspaceId>/lakehouses/<SilverLakehouseId>`. *Easier:* delete the `Source` step on `SilverProduction` and re-create it via **Get data → Lakehouse → `Silver_LH` → `production_conformed`**.
4. On `GoldProductionDaily` set **data destination → Lakehouse → `Gold_LH` → `production_daily`**, Update method **Replace**, then **Publish**.

   (Manual fallback: paste each `let … in …` body from `DF_Gold_PA.m` into a Blank query — omit the `section`/`shared` lines.)

**8. Create the semantic model `Gold_SM`** — `semantic_model/Gold_SM.bim` is a DirectLake TMSL model: tables `production_daily`, `cost_monthly`, `schedule_summary`, `field_kpi_facts` + a calculated `DateDim`, with measures `Total BOE` and `Total Cost (USD)`.
- **From the `.bim` (recommended):** open in Tabular Editor / Power BI Desktop, point the DirectLake source at this workspace's `Gold_LH` SQL endpoint, deploy as `Gold_SM`. Publish from modern tooling so the model has **Enhanced Metadata** (mandatory for Deployment Pipelines). Copy its GUID for the `SemanticModelId` Deployment Rule.
- **From Fabric (click-through):** open `Gold_LH` → **New semantic model** → tick the Gold tables → Confirm. Add `DateDim` as a calculated table `= CALENDAR(DATE(2024,1,1), DATE(2024,12,31))`, the relationship to `production_daily[date]`, and the two measures.

> ⚠️ **`production_daily` missing from the table picker?** Tables written by **Dataflow Gen2** (`production_daily`) appear in the SQL analytics endpoint / OneLake picker **after a metadata sync**, while Spark/notebook tables show immediately. If the New-semantic-model dialog lists only `cost_monthly` / `schedule_summary` / `field_kpi_facts`: open **`Gold_LH` → SQL analytics endpoint**, click **Refresh** (or the ⟳ icon in the table picker), wait a few seconds, and re-open the dialog. The `.bim` already defines `production_daily`, so the Git/`.bim` path is unaffected — this is only the live picker lagging.

**9. Build the report `Gold_Dashboard`** — New → Report → live-connect to `Gold_SM` (DirectLake, never Import). Add cards for `Total BOE` / `Total Cost (USD)`, a line chart by `DateDim[Date]`, and a column chart by `field`. Save as `Gold_Dashboard`.

---

## Part C — CI/CD

> Part C1 (Steps 10–13) is the **manual** Dev → Prod promotion you can test right now — no service principal, no GitHub Actions. Part C2 (Steps 14–16) adds automation later.

### Part C1 — Manual promotion with a Deployment Pipeline

**10. Connect Git & commit** — Workspace settings → Git integration → GitHub → connect → **Update**. Then Source control → select items → **Commit**.

**11. Create the Deployment Pipeline** — Fabric portal → Deployment pipelines → New → two stages (**Development → Production**); assign `ws-CICD-Dev` to Development and `ws-CICD-Prod` (empty) to Production. In the **Development** stage select **all** items and click **Deploy** *once*. Fabric creates paired copies in Prod and links them. **Deploy everything in a single pass** — items deployed in separate batches don't auto-pair and create duplicates. Copy the **pipeline GUID** from the URL (needed later for automation).

**12. How each item maps Dev → Prod** — the deploy pairs items by name and re-points most bindings automatically. Out of all 10 items, **only the Dataflow needs a manual connection edit** and **only the semantic model needs a rule**:

| Item | Auto-binds on deploy? | What you do in Prod |
| --- | --- | --- |
| 3 Lakehouses (`Bronze_LH`/`Silver_LH`/`Gold_LH`) | Paired by name | Structure copies, **data does not** — reload (Step 13) |
| 4 Notebooks (`NB_Setup`, `NB_01`–`03`) | Default lakehouse re-maps to paired Prod LH | Re-attach lakehouses if the pane is blank (Step 4). 3-part names (`Silver_LH.dbo.…`) resolve by name |
| 1 Dataflow (`DF_Gold_PA`) | ⚠️ **No** — source/destination still point at **Dev** lakehouse GUIDs | **Open it in Prod, repoint** `SilverProduction` source → Prod `Silver_LH` and `GoldProductionDaily` destination → Prod `Gold_LH` (just set the two `Silver*Id` params), **Publish** |
| 1 Semantic model (`Gold_SM`) | DirectLake re-binds to paired Prod `Gold_LH` | Add the **Data source rule** below to make it explicit; measures travel with the model |
| 1 Report (`Gold_Dashboard`) | Re-binds to paired Prod `Gold_SM` | Confirm it shows Prod data |

**Configure the one rule** (`deployment_rules/deployment_rules.json` is the reference) — on the **Production** stage click **⚙️ Deployment rules** → select `Gold_SM` → **Data source rule** → point its Lakehouse / SQL endpoint at `ws-CICD-Prod/Gold_LH`, **Save**.

| Item | Rule | Set |
| --- | --- | --- |
| `Gold_SM` | Data source | Bind to `ws-CICD-Prod/Gold_LH` |

> **No rule is needed for `PL_Refresh_Master`'s item references.** The pipeline points at notebooks/dataflow by literal GUID; when you promote **all items in the same deploy**, the Deployment Pipeline **auto-pairs** them and rewrites those GUIDs to the Prod items automatically on every deploy. The only manual rule is the `Gold_SM` data-source binding. Rules are portal-only (no public REST API) and apply automatically on every deploy.

**13. Reload data & verify the mapping** — lakehouse deploys carry **structure only, no data**, so populate Prod and check every binding:

1. **Pairing** — in the pipeline compare view every item shows the *paired* (chain-link) icon, none show "different"/"only in source"; no duplicate same-name items.
2. **Lakehouses** — in Prod re-attach lakehouses in `NB_01`/`NB_02`/`NB_03` (Step 4).
3. **Repoint the Dataflow** — open `DF_Gold_PA` in Prod, set `SilverWorkspaceId`/`SilverLakehouseId` to the **Prod** `Silver_LH`, confirm the `GoldProductionDaily` destination is **Prod** `Gold_LH`, **Publish**.
4. **Load** — run `NB_Setup → NB_01 → NB_02`, refresh `DF_Gold_PA`, then run `NB_03` (or run `PL_Refresh_Master` once now that the Dataflow points at Prod).
5. **Semantic model** — open `Gold_SM` → Settings/lineage shows **Prod** `Gold_LH` (not Dev); refresh; `Total BOE` / `Total Cost (USD)` return values.
6. **Report** — open `Gold_Dashboard`; visuals render from Prod data and lineage points at the Prod `Gold_SM`.

> **Why the Dataflow is the one manual step:** notebooks, semantic models and reports use Fabric **internal item references** that the deployment pipeline rewrites to the paired Prod items. Dataflow Gen2 source/destination use **connection objects** that aren't part of that auto-rebind, so they keep pointing at Dev until you repoint them. Parameterizing those IDs is the first automation win in Part C2.

### Part C2 — Automate the promotion (later)

**14. Service principal & GitHub secrets** — register an Entra app (client ID, tenant ID, secret); enable *"Service principals can use Fabric APIs"*; add the SP as Admin on both workspaces and the deployment pipeline. Add GitHub secrets: `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_PIPELINE_ID` (the pipeline GUID from Step 11).

**15. Add the workflow** — copy `github_actions/deploy-dev-to-prod.yml` to `.github/workflows/`. On push to `main` it gets a Fabric token, calls `POST /v1/pipelines/{id}/deploy` (Dev→Prod), and gates on the GitHub `production` environment (add required reviewers). The `Gold_SM` Deployment Rule applies server-side; the Dataflow repoint (Step 13.3) and data reload (Step 13.4) still run after each deploy until parameterized.

**16. Demo the loop** — edit `NB_02` (e.g. uncomment the `gor_ratio` KPI) → run → Source control → Commit → PR to `main` → merge → GitHub Actions promotes Dev→Prod after reviewer approval.

---

## Efficient end-to-end CI/CD (how this repo is wired)

- **Native item references** — the pipeline references notebooks/dataflow by literal GUID, exactly as Fabric exports them, so the template imports and saves without hanging. (Expression-based `notebookId`/`workspaceId` is what breaks the importer.)
- **Promotion auto-pairs items** — build everything in Dev, assign workspaces, then deploy once: the Deployment Pipeline pairs every item and rewrites the pipeline's GUID references to the Prod items automatically on every subsequent deploy. No per-stage parameter wiring for item IDs.
- **One manual rule** — only the `Gold_SM` data-source binding is set as a deployment rule; everything else item-related auto-pairs.
- **One manual repoint** — the Dataflow Gen2 source/destination connections don't auto-rebind, so `DF_Gold_PA` is repointed once per target workspace (Step 13.3). Parameterizing those IDs is the first automation win.
- **One-command refresh** — `PL_Refresh_Master` runs `NB_Setup → NB_01 → NB_02 → DF_Gold_PA → NB_03` in dependency order, so post-deploy data load is a single click (or one REST call).
- **One artifact, two delivery paths** — the `.json` is the Git/source reference; the `.zip` is the one-click import template. Same activities, same order, same literal-GUID shape.

## Key limitations

| Area | Limitation | Mitigation |
| --- | --- | --- |
| Git | Sensitivity labels block commits; 50 MB/commit; Admin-only connect; no MyWorkspace | Remove labels; batch commits; pre-configure; use named workspaces |
| Deploy | Lakehouse copies structure, not data | Run `PL_Refresh_Master` after each deploy |
| Deploy | Items added after assignment aren't auto-paired; same-name unpaired items duplicate | Build all items first, verify pairing before deploying |
| Deploy | Semantic models need Enhanced Metadata | Publish from modern Power BI Desktop / Tabular Editor |
| Notebook | Attached-lakehouse GUIDs are workspace-specific | Keep `dependencies` empty in Git; re-attach per workspace (Step 4) |
| Dataflow | Source/destination connections don't auto-rebind on deploy | Repoint `DF_Gold_PA` to the Prod lakehouses once after deploy (Step 13.3) |
| Dataflow | New tables lag the SQL endpoint / picker | Refresh the Gold_LH SQL endpoint (Step 8 note) |
