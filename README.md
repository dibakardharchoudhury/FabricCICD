# Microsoft Fabric CI/CD — Medallion Demo

A Bronze → Silver → Gold lakehouse that is promoted between Fabric workspaces
**entirely from code** with [fabric-cicd](https://microsoft.github.io/fabric-cicd/).
One notebook — `NB_04_Deploy` — publishes every item in this repo into a target
workspace and rebinds all cross-workspace references. No manual clicks, no Deployment Pipeline.

## What's in the pipeline

```
Bronze_LH ─ NB_01_Seed_Bronze        (inline sample data — no external source)
   └► Silver_LH ─ NB_02_Transform_Silver
        └► Gold_LH ─ DF_Gold_PA → NB_03_Aggregate_Gold
              └► Gold_SM (Direct Lake) ─► Gold_Dashboard
```

`PL_Refresh_Master` runs the whole chain in order; `NB_03_Aggregate_Gold` refreshes `Gold_SM` right after it writes the Gold tables (the pipeline has no separate model-refresh activity).

## Repo layout

| Path | What it is |
| --- | --- |
| `Bronze_LH` / `Silver_LH` / `Gold_LH` `.Lakehouse/` | Medallion lakehouses |
| `NB_01_Seed_Bronze` … `NB_03_Aggregate_Gold` `.Notebook/` | Transform notebooks |
| `DF_Gold_PA.Dataflow/` | Builds Gold `production_daily` (Dataflow Gen2) |
| `PL_Refresh_Master.DataPipeline/` | Orchestrates Bronze→Gold (NB_03 refreshes the model) |
| `Gold_SM.SemanticModel/` | Direct Lake (on OneLake) semantic model |
| `Gold_Dashboard.Report/` | Report built on `Gold_SM` |
| `semanticlink.Environment/` | Spark env (pins `fabric-cicd`, `semantic-link-labs`) |
| `NB_04_Deploy.Notebook/` | **The deploy tool** — publishes everything above to a stage |

Every item is in **Fabric Git source format** — they appear here when you Git-connect a
workspace and **commit from Fabric**.

## One-time setup

1. **Fork this repo** (or create your own copy) and **Git-connect** your **Dev** workspace to
   your fork. You commit your changes to your fork, not to the upstream repo.
2. Build the items in Dev (or sync them from your fork), then **commit from Fabric** so the
   repo holds the source format.
3. Create a **target** workspace for each stage (e.g. `ws-CICD-PROD`). It can be empty —
   `NB_04_Deploy` creates the lakehouse shells.
4. The identity running `NB_04_Deploy` must be **Admin/Member on both workspaces** (workspace
   Admin/Member can already modify `Gold_SM`, so no separate model ownership is needed).
5. *(Only when running inside Fabric)* store a repo-scoped **GitHub PAT** in **Azure Key Vault**
   so the Spark node can clone the repo. Not needed locally or in CI.

## Deploy to a stage

1. Open `NB_04_Deploy` and set the **parameters** cell:
   - `target_workspace_name` — the stage to deploy into.
   - `environment` — the stage label, e.g. `"Production"`.
   - `dev_workspace_name` — the source workspace whose GUIDs are translated.
   - In Fabric: also set `git_repo_url`, `key_vault_url`, `git_pat_secret`.
   - Local / CI: leave `local_repo_path = ""` to auto-detect the checked-out repo.
2. **Run all cells.** `NB_04_Deploy` publishes every item, rewrites all Dev GUIDs to the
   target stage, and rebinds the Direct Lake model to the target's `Gold_LH`.
3. **One-time per stage — sign in to the `DF_Gold_PA` destination.** fabric-cicd publishes the
   dataflow definition but **cannot bind its Dataflow Gen2 connection** (connections are
   tenant-level and not auto-mapped). On a stage's first deploy, open **`DF_Gold_PA` → Edit →
   `GoldProductionDaily` → Data destination** (gear), confirm it points at this stage's `Gold_LH`
   / `production_daily` (Replace), **sign in**, and **Save**. You only do this once per stage; later
   deploys reuse the connection.
4. In the target workspace, run **`PL_Refresh_Master`** once. Git never carries table data,
   so this (re)creates the tables, loads data, and `NB_03` refreshes `Gold_SM`.

That's it — `Gold_Dashboard` in the target now shows live data.

## What NB_04 does

1. **Discovers** every item in the repo and resolves its Dev GUIDs by name.
2. **Generates `parameter.yml`** so fabric-cicd rewrites each Dev workspace/lakehouse/item
   GUID to the target stage as it publishes.
3. **Publishes** all items, then **rebinds** the Direct Lake model in code.

Nothing is hardcoded — add an item to the repo and it is picked up automatically.

## The change loop

1. Edit an item in **Dev**.
2. **Commit from Fabric** → push to this repo.
3. Re-run `NB_04_Deploy` against the stage — only changed items are updated.

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

- **Table data isn't in Git** — only the lakehouse container is. Always run
  `PL_Refresh_Master` after a deploy.
- **Direct Lake on OneLake** can't be rebound by a deployment rule, so `NB_04` does it in
  code (needs `semantic-link-labs`; workspace Admin/Member can rebind the model).
- **The `DF_Gold_PA` connection isn't deployed** — fabric-cicd can't bind a Dataflow Gen2
  connection, so the destination needs a **one-time sign-in per stage** (see step 3 above).
- **`parameter.yml` is generated at deploy time** and not checked in — keep
  `generate_parameter_yml = True`.
- Items pair across stages by **name** — keep display names identical between workspaces.

## Troubleshooting

### `DF_Gold_PA` refresh fails with "Something went wrong" / "credentials are missing or invalid"

The pipeline surfaces a generic `ActionUserFailure` ("Something went wrong, please try again
later"); the dataflow's **Refresh history** shows `Data source credentials are missing or invalid`
(code `999999`) on the `GoldProductionDaily_WriteToDataDestination` step.

This is almost always **transient**, not a broken credential or a query bug. The detailed mashup
logs reveal the real cause: a SQL login failure (`18456 / InvalidCredentials`) opening the Gen2
**internal staging warehouse** (`StagingLakehouseForDataflows_*`) with the message *"Couldn't
complete the operation due to a system update. Close out this connection, sign in again, and retry."*
A Fabric backend update rolled mid-refresh and invalidated the in-flight token — the source query
itself succeeds, only the write/staging step fails.

Fix, in order:
1. **Re-run** the dataflow (or `PL_Refresh_Master`). It usually clears on the next refresh.
2. If it persists, open **`DF_Gold_PA` → Edit → sign in again** to re-auth the Lakehouse
   connection, **Save**, and refresh.
3. If it recurs across runs, bind the connection to a **service principal** instead of a personal
   OAuth token so a backend roll doesn't drop the session.

> The `404 The specified path does not exist` lines in the logs are normal existence probes, not
> the failure.

### `Gold_SM` refresh fails on a stage's **first** run (`0xC14700DF` "tables do not exist or access denied")

On the first deploy to a new stage, `NB_03_Aggregate_Gold` creates the Gold tables **from scratch**.
New Delta objects (especially `field_kpi_facts`) take several minutes to register into OneLake
metadata before Direct Lake can frame them — until then the refresh returns `0xC14700DF`. This is
**metadata-sync lag, not a permission problem** (the same identity succeeds once sync completes).
`NB_03` already retries over a long window (~28 min) to ride this out; just let it run, or re-run the
notebook later. Steady-state runs overwrite data only (table identity preserved) and refresh on the
first attempt.

