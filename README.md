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

`PL_Refresh_Master` runs the whole chain in order and refreshes `Gold_SM` at the end.

## Repo layout

| Path | What it is |
| --- | --- |
| `Bronze_LH` / `Silver_LH` / `Gold_LH` `.Lakehouse/` | Medallion lakehouses |
| `NB_01_Seed_Bronze` … `NB_03_Aggregate_Gold` `.Notebook/` | Transform notebooks |
| `DF_Gold_PA.Dataflow/` | Builds Gold `production_daily` (Dataflow Gen2) |
| `PL_Refresh_Master.DataPipeline/` | Orchestrates Bronze→Gold + model refresh |
| `Gold_SM.SemanticModel/` | Direct Lake (on OneLake) semantic model |
| `Gold_Dashboard.Report/` | Report built on `Gold_SM` |
| `semanticlink.Environment/` | Spark env (pins `fabric-cicd`, `semantic-link-labs`) |
| `NB_04_Deploy.Notebook/` | **The deploy tool** — publishes everything above to a stage |

Every item is in **Fabric Git source format** — they appear here when you Git-connect a
workspace and **commit from Fabric**.

## One-time setup

1. Create a **Dev** workspace and **Git-connect** it to this repo.
2. Build the items in Dev (or sync them from this repo), then **commit from Fabric** so the
   repo holds the source format.
3. Create a **target** workspace for each stage (e.g. `ws-CICD-PROD`). It can be empty —
   `NB_04_Deploy` creates the lakehouse shells.
4. The identity running `NB_04_Deploy` must be **Admin/Member on both workspaces** and
   **own `Gold_SM`** (required to rebind Direct Lake).
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
3. In the target workspace, run **`PL_Refresh_Master`** once. Git never carries table data,
   so this (re)creates the tables, loads data, and refreshes `Gold_SM`.

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
  code (needs `semantic-link-labs` and ownership of the model).
- **`parameter.yml` is generated at deploy time** and not checked in — keep
  `generate_parameter_yml = True`.
- Items pair across stages by **name** — keep display names identical between workspaces.
