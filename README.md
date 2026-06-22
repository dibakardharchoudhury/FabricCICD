# Microsoft Fabric CI/CD — Medallion Demo

A Bronze → Silver → Gold lakehouse promoted between Fabric workspaces **entirely from code**
with [fabric-cicd](https://microsoft.github.io/fabric-cicd/). One notebook — `NB_04_Deploy` —
publishes every repo item into a target workspace and rebinds all cross-workspace references.
No manual clicks, no Deployment Pipeline.

## Pipeline

```
Bronze_LH ─ NB_01_Seed_Bronze        (inline sample data)
   └► Silver_LH ─ NB_02_Transform_Silver
        └► Gold_LH ─ DF_Gold_PA → NB_03_Aggregate_Gold
              └► Gold_SM (Direct Lake) ─► Gold_Dashboard
```

`PL_Refresh_Master` runs the chain in order; `NB_03` refreshes `Gold_SM` right after writing the
Gold tables (no separate model-refresh activity).

## Repo layout

| Path | What it is |
| --- | --- |
| `Bronze_LH` / `Silver_LH` / `Gold_LH` `.Lakehouse/` | Medallion lakehouses |
| `NB_01` … `NB_03` `.Notebook/` | Transform notebooks |
| `DF_Gold_PA.Dataflow/` | Builds Gold `production_daily` (Dataflow Gen2) |
| `PL_Refresh_Master.DataPipeline/` | Orchestrates Bronze→Gold |
| `Gold_SM.SemanticModel/` | Direct Lake (on OneLake) model |
| `Gold_Dashboard.Report/` | Report on `Gold_SM` |
| `semanticlink.Environment/` | Spark env (`fabric-cicd`, `semantic-link-labs`) |
| `NB_04_Deploy.Notebook/` | **Deploy tool** — publishes everything to a stage |

Items are in **Fabric Git source format** — produced when you Git-connect a workspace and
**commit from Fabric**.

## Steps

1. **Fork** this repo and **Git-connect** your **Dev** workspace to your fork (commit to your fork, not upstream).
2. Build the items in Dev (or sync from your fork).
3. **Publish the `semanticlink` Environment** (one-time per workspace). A new Environment is
   unpublished until built, so `semantic-link-labs` / `sempy_labs` is unavailable and
   `NB_03_Aggregate_Gold` fails with `ModuleNotFoundError: No module named 'sempy_labs'`. Open
   **`semanticlink` → Publish** and wait (~10–20 min). Re-publish only when its libraries change.
4. **Sign in to the `DF_Gold_PA` destination** (one-time per stage) — fabric-cicd can't bind a
   Dataflow Gen2 connection, and it differs per tenant. Open **`DF_Gold_PA` → Edit →
   `GoldProductionDaily` → Data destination**, confirm it points at the stage's `Gold_LH` /
   `production_daily` (Replace), **sign in**, **Save**.
5. **Validate in Dev, then deploy to Prod:**
   - In **Dev**, run **`PL_Refresh_Master`** (Bronze→Silver→Gold) and open **`Gold_Dashboard`** to confirm it loads.
   - Create an empty **Prod** workspace (e.g. `ws-CICD-PROD`); `NB_04_Deploy` creates the lakehouse shells.
   - Open **`NB_04_Deploy`**, set the **parameters** cell (`target_workspace_name`, `environment`,
     `dev_workspace_name`; in Fabric also `git_repo_url`, `key_vault_url`, `git_pat_secret`; local / CI
     leave `local_repo_path = ""`), and **Run all cells**.
   - **Publish the Environment in Prod** (once per stage).
   - Run **`PL_Refresh_Master`** in Prod once — Git carries no table data, so this loads the tables and
     refreshes `Gold_SM`. `Gold_Dashboard` now shows live data.
6. The identity running `NB_04_Deploy` must be **Admin/Member on both workspaces** (enough to modify `Gold_SM` — no separate ownership).
7. *(In Fabric only)* store a repo-scoped **GitHub PAT** in **Azure Key Vault** for the clone. Not needed locally / CI.

## What NB_04 does

**Discovers** every repo item and resolves its Dev GUIDs by name → **generates `parameter.yml`** so
fabric-cicd rewrites each Dev workspace/lakehouse/item GUID to the target → **publishes** all items
and **rebinds** the Direct Lake model in code. Nothing is hardcoded; add an item and it's picked up
automatically.

## Change loop

Edit in **Dev** → **commit from Fabric** → re-run `NB_04_Deploy` (only changed items update).

## Key NB_04 parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `target_workspace_name` | `ws-CICD-PROD` | Stage to deploy into |
| `environment` | `Production` | Stage label used by `parameter.yml` |
| `dev_workspace_name` | `ws-CICD-DevTest` | Source workspace (GUIDs → tokens) |
| `generate_parameter_yml` | `True` | Auto-build `parameter.yml` from the repo |
| `rebind_direct_lake` | `True` | Re-point Direct Lake models to the target lakehouse |
| `include_lakehouses` | `True` | Deploy lakehouses from the repo |
| `remove_orphans` | `False` | Delete target items no longer in Git |

## Good to know

- **Table data isn't in Git** — only the lakehouse container. Always run `PL_Refresh_Master` after a deploy.
- **A new Environment must be published per stage** before the pipeline runs (Steps 3 / 5).
- **Direct Lake on OneLake** can't be rebound by a deployment rule, so `NB_04` does it in code (`semantic-link-labs`; Admin/Member suffices).
- **The `DF_Gold_PA` connection isn't deployed** — needs a **one-time sign-in per stage** (Step 4).
- **`parameter.yml` is generated at deploy time**, not checked in — keep `generate_parameter_yml = True`.
- Items pair across stages by **name** — keep display names identical.

## Troubleshooting

### `DF_Gold_PA` refresh: "Something went wrong" / "credentials are missing or invalid"

Generic `ActionUserFailure`; Refresh history shows `Data source credentials are missing or invalid`
(`999999`) on `GoldProductionDaily_WriteToDataDestination`. Usually **transient**: a SQL login
failure (`18456`) opening the Gen2 staging warehouse (`StagingLakehouseForDataflows_*`) after a
mid-refresh backend update invalidated the token \u2014 the source query succeeds, only the write fails.
Fix in order:
1. **Re-run** the dataflow / `PL_Refresh_Master` \u2014 usually clears next run.
2. If it persists, **`DF_Gold_PA` \u2192 Edit \u2192 sign in again**, **Save**, refresh.
3. If recurring, bind the connection to a **service principal** so a backend roll doesn't drop the session.

> `404 ... path does not exist` lines are normal existence probes, not the failure.

### `Gold_SM` refresh fails on a stage's **first** run (`0xC14700DF`)

First deploy: `NB_03` creates the Gold tables from scratch; new Delta objects (esp.
`field_kpi_facts`) take minutes to register in OneLake metadata before Direct Lake can frame them.
This is **metadata-sync lag, not permissions**. `NB_03` retries ~28 min \u2014 let it run or re-run later.
Steady-state runs overwrite data only and refresh first try.

### `NB_03_Aggregate_Gold` fails: `ModuleNotFoundError: No module named 'sempy_labs'`

The `semanticlink` Environment is deployed but **unpublished**, so `semantic-link-labs` isn't
installed on the Spark pool. Fix (one-time per stage): **`semanticlink` \u2192 Publish** (~10\u201320 min),
then re-run `PL_Refresh_Master`. Re-publish only when libraries change.

