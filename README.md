# Microsoft Fabric CI/CD — Medallion Demo

A Bronze → Silver → Gold lakehouse promoted between Fabric workspaces **entirely from code**
with [fabric-cicd](https://microsoft.github.io/fabric-cicd/). One notebook — `NB_04_Deploy` —
publishes the production item definitions into a target workspace and rebinds all cross-workspace
references. The `Deploy/` subtree itself stays in Dev.
No Fabric Deployment Pipeline is required; Dataflow credentials still require a one-time sign-in
in each workspace.

## Pipeline

```text
Bronze_LH ─ NB_01_Seed_Bronze        (inline sample data)
   └► Silver_LH ─ NB_02_Transform_Silver
        └► Gold_LH ─ DF_Gold_PA → NB_03_Aggregate_Gold
              └► Gold_SM (Direct Lake) ─► Gold_Dashboard
```

`PL_Refresh_Master` runs the chain in order; `NB_03` refreshes `Gold_SM` right after writing the
Gold tables (no separate model-refresh activity). Each Notebook activity must use a Fabric Notebook
connection authenticated as `fabric-rest` to run independently of the pipeline caller. The Dataflow
uses its separately configured connection credential.

## Repo layout

| Path | What it is |
| --- | --- |
| `Bronze/` | `Bronze_LH` and `NB_01_Seed_Bronze` |
| `Silver/` | `Silver_LH` and `NB_02_Transform_Silver` |
| `Gold/` | `Gold_LH`, `DF_Gold_PA`, `NB_03`, pipeline, model, and report |
| `Deploy/NB_04_Deploy.Notebook/` | **Dev-only deploy tool** — excluded from target stages |
| `semanticlink.Environment/` | Spark env (`fabric-cicd`, `semantic-link-labs`) |

Items are in **Fabric Git source format** — produced when you Git-connect a workspace and
**commit from Fabric**.

## End-to-end setup

There are two different first-time paths:

- **Fresh Dev from Git:** use Part A. Git creates the item definitions, but notebook bindings,
  Environment publication, Dataflow credentials, and all table data still need bootstrapping.
- **Fresh target deployed by `NB_04_Deploy`:** use Part B. `NB_04` deploys and rebinds the item
  definitions, but the Lakehouses are still empty and the Dataflow destination needs a sign-in.

Do not configure the Dataflow destination while every Lakehouse is empty. Its source is
`Silver_LH.dbo.production_conformed`; until that table exists, Fabric shows **"source query did not
return a table"**, cannot calculate column mappings, and disables **Save settings**.

### Key Vault private networking prerequisite

`NB_04_Deploy` is the **only tracked notebook in this repository that reads Azure Key Vault**. It
calls `notebookutils.credentials.getSecret(...)` to retrieve the GitHub PAT before cloning the repo
when it runs inside Fabric. `NB_01`, `NB_02`, and `NB_03` do not read Key Vault.

If the Key Vault blocks public network access, create a Fabric **managed private endpoint** to that
vault in every workspace where `NB_04_Deploy` can execute. Managed private endpoints are workspace
scoped; an endpoint created in Dev is not inherited by Test or Prod.

| Workspace | Key Vault endpoint required? |
| --- | --- |
| Dev | Yes, when `NB_04_Deploy` runs in Dev |
| Test | Yes, when `NB_04_Deploy` runs in Test |
| Prod | Yes, when `NB_04_Deploy` runs in Prod |
| Target-only workspace | No, if `NB_04_Deploy` runs elsewhere and only publishes into this workspace |
| Local machine / CI runner | No Fabric endpoint; the checked-out repo is used and Key Vault cloning is skipped |

For a consistent Dev/Test/Prod operating model, provision and approve the endpoint in all three
workspaces before the first deployment:

1. In Azure, confirm the `Microsoft.Network` resource provider is registered in the subscription.
2. Copy the Key Vault resource ID from **Azure portal → Key Vault → Properties**. Its format is:

   ```text
   /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.KeyVault/vaults/<vault-name>
   ```

3. In the Fabric **Dev** workspace, open **Workspace settings → Network security → Managed private
   endpoints → Create**.
4. Enter a unique endpoint name, paste the Key Vault resource ID, and create the request. Use the
   Azure resource ID, not the `https://<vault-name>.vault.azure.net/` URL.
5. In Azure, open **Key Vault → Networking → Private endpoint connections**, select the pending
   Fabric request, and approve it.
6. Return to the Fabric workspace's **Network security** page, refresh it, and wait until provisioning
   is successful and the connection status is **Approved**. A created or pending endpoint is not ready.
7. Repeat Steps 3–6 independently for **Test** and **Prod**. The same Key Vault then has three private
   endpoint connections, one from each Fabric workspace.
8. Grant the identity that actually runs `NB_04_Deploy` permission to read the PAT secret. With Azure
   RBAC, use the least-privilege **Key Vault Secrets User** role; with legacy access policies, grant
   secret **Get** permission. Network approval and secret authorization are both required.
9. In each execution workspace, run this preflight without printing the secret:

   ```python
   notebookutils.credentials.getSecret(
       "https://<vault-name>.vault.azure.net/",
       "<github-pat-secret-name>",
   )
   print("Key Vault network and secret access succeeded")
   ```

Do not run `NB_04_Deploy` in a workspace until this preflight succeeds there. If a pipeline or service
account submits the notebook, grant access to that submitting identity and test in that execution
context; an interactive test under a different user does not validate the pipeline identity.

### Part A — Set up a fresh Dev workspace from Git

1. **Fork and connect the repo.** Fork this repository, create/open the Dev workspace, and connect
   Fabric Git integration to your fork and branch. Sync the workspace from Git.

2. **Attach the notebook Lakehouses.** Git carries the original workspace GUIDs, which do not resolve
   in a different workspace. In each notebook's Lakehouses pane, attach these items and set the default:

   | Notebook | Attach | Default |
   | --- | --- | --- |
   | `NB_01_Seed_Bronze` | `Bronze_LH` | `Bronze_LH` |
   | `NB_02_Transform_Silver` | `Bronze_LH`, `Silver_LH` | `Silver_LH` |
   | `NB_03_Aggregate_Gold` | `Silver_LH`, `Gold_LH` | `Gold_LH` |

   `NB_04_Deploy` does not need a Lakehouse attachment.

3. **Publish the `semanticlink` Environment.** Open `semanticlink` and select **Publish**. Wait for
   the build to finish (~10–20 minutes). Git sync creates the Environment definition but does not
   build its libraries. `NB_03` needs `semantic-link-labs` / `sempy_labs`.

4. **Create the Bronze source tables.** Run `NB_01_Seed_Bronze` and verify these tables exist under
   `Bronze_LH.dbo`:

   - `production_raw`
   - `cost_raw`
   - `schedule_raw`

5. **Create the Silver source tables.** Run `NB_02_Transform_Silver` and verify these tables exist
   under `Silver_LH.dbo`:

   - `production_conformed`
   - `cost_conformed`
   - `schedule_conformed`

6. **Configure the Dataflow source.** Open `DF_Gold_PA` in edit mode. Set its parameters to the Dev
   workspace's actual IDs:

   - `SilverWorkspaceId` = the current Dev workspace ID
   - `SilverLakehouseId` = the current Dev `Silver_LH` item ID

   Select `SilverProduction` and refresh its preview. **Do not continue until rows and columns appear.**
   If it returns no table, recheck the two IDs and confirm the Silver tables were created in the
   preceding step.

7. **Configure the Dataflow destination.** Select `GoldProductionDaily`, then **Data destination**:

   - Choose **New table**. The table should not exist on a fresh setup.
   - Select the current Dev workspace → `Gold_LH` → `dbo`.
   - Enter table name `production_daily` and select **Next**.
   - Leave **Use automatic settings** enabled. Confirm column mappings are displayed.
   - Choose the **Replace** update method when shown, sign in to the Lakehouse connection, and select
     **Save settings**.
   - Save/publish the Dataflow.

   If **Save settings** is disabled with "source query did not return a table," cancel the destination
   dialog and return to Step 6. The destination cannot be configured until the Silver preview works.

8. **Run the Dataflow once.** Refresh `DF_Gold_PA` and verify
   `Gold_LH.dbo.production_daily` now exists. This table must exist before `NB_03` runs.

9. **Run the Gold notebook.** Run `NB_03_Aggregate_Gold`. It requires `production_daily`, creates:

   - `cost_monthly`
   - `schedule_summary`
   - `field_kpi_facts`

   It then rebinds and refreshes the Direct Lake semantic model `Gold_SM`. On the first run, newly
   created Delta tables can take several minutes to register; let the built-in refresh retry continue.

10. **Validate Dev end to end.** Run `PL_Refresh_Master`. It should complete this fixed sequence:

    ```text
    NB_01_Seed_Bronze
      → NB_02_Transform_Silver
      → DF_Gold_PA
      → NB_03_Aggregate_Gold
      → Gold_SM refreshed by NB_03
    ```

    Open `Gold_Dashboard` and confirm the visuals contain data. From now on, run
    `PL_Refresh_Master`; the manual notebook/Dataflow sequence above is only for first-time setup.

### Part B — Deploy and bootstrap a fresh target workspace

1. **Complete Part A in Dev first.** Dev must work end to end before it is used as the deployment source.

2. **Commit from Fabric to Git.** Commit the validated Dev item definitions to your fork. Table data is
   never committed; only item and Lakehouse definitions are stored in Git.

3. **Prepare deployment authentication and networking.** The identity running `NB_04_Deploy` must be
   Admin/Member on both Dev and the target workspace. For an in-Fabric run, store a fine-grained,
   repo-scoped GitHub PAT in Azure Key Vault, complete the **Key Vault private networking prerequisite**
   above for the workspace running the notebook, and pass its preflight. A local/CI run can use the
   existing checkout and does not need the PAT or a Fabric managed private endpoint.

4. **Create an empty target workspace.** For example, create `ws-FabricCICD-PROD`. Do not manually create
   its Fabric items; `NB_04_Deploy` creates them from Git.

5. **Run `NB_04_Deploy`.** In its parameters section, set:

   - `target_workspace_name` to the target workspace
   - `environment` to `Production`
   - `dev_workspace_name` to the validated Dev workspace
   - For an in-Fabric run: `git_repo_url`, `key_vault_url`, and `git_pat_secret`
   - For local/CI: leave `local_repo_path = ""` to auto-detect the checkout

   Run all sections. `NB_04` creates/updates the Fabric items, rewrites cross-workspace references,
   rebinds `Gold_SM` to the target `Gold_LH`, and publishes the `semanticlink` Environment. The first
   Environment build can take ~20 minutes.

6. **Confirm target bindings.** In the target workspace, verify the three notebook attachments match
   the table in Part A Step 2. Open `DF_Gold_PA` and verify `SilverWorkspaceId` and
   `SilverLakehouseId` point to the target workspace and its `Silver_LH`; `NB_04` normally rewrites
   these automatically.

7. **Bootstrap Bronze and Silver in the target.** The deployed Lakehouses are empty. Run
   `NB_01_Seed_Bronze`, then `NB_02_Transform_Silver`. Confirm
   `Silver_LH.dbo.production_conformed` exists before opening the Dataflow destination settings.

8. **Confirm and authenticate the Dataflow destination.** Open `DF_Gold_PA` → `GoldProductionDaily`
   → **Data destination**. It must target the target workspace's `Gold_LH.dbo.production_daily` with
   Replace behavior. Sign in to the target Lakehouse connection and save/publish the Dataflow.

   On an empty target, choose **New table**. If mappings are unavailable, return to Step 7 and verify
   the `SilverProduction` preview returns rows. Deployment can rewrite destination IDs, but it cannot
   deploy a user's Dataflow connection credential.

9. **Run the target pipeline.** Run `PL_Refresh_Master`. Re-running `NB_01` and `NB_02` is intentional
   and safe. The pipeline creates `production_daily`, then runs `NB_03` to create the remaining Gold
   tables and refresh `Gold_SM`.

10. **Validate the target.** Confirm all four tables exist under `Gold_LH.dbo`:

    - `production_daily`
    - `cost_monthly`
    - `schedule_summary`
    - `field_kpi_facts`

    Open `Gold_Dashboard` and verify it displays current data.

### First-run checkpoints

| Before this action | This must already be true |
| --- | --- |
| Configure `DF_Gold_PA` destination | `SilverProduction` preview returns a table |
| Refresh `DF_Gold_PA` | Source IDs are correct and destination connection is signed in |
| Run `NB_03_Aggregate_Gold` | `Gold_LH.dbo.production_daily` exists |
| Open `Gold_Dashboard` | `NB_03` completed and refreshed `Gold_SM` |
| Use only `PL_Refresh_Master` for future runs | One-time Dataflow setup is complete |

## What NB_04 does

**Discovers** every production item outside `Deploy/` and resolves its Dev GUIDs by name → **generates `parameter.yml`** so
fabric-cicd rewrites each Dev workspace/lakehouse/item GUID to the target → **publishes** all items
and **rebinds** the Direct Lake model in code. Nothing is hardcoded; add an item and it's picked up
automatically.

## Change loop

Edit in **Dev** → **commit from Fabric** → merge to `main` → GitHub deploys only affected items.
Use `NB_04_Deploy` for a manual in-Fabric deployment.

## Key NB_04 parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `target_workspace_name` | `ws-FabricCICD-PROD` | Stage to deploy into |
| `environment` | `Production` | Stage label used by `parameter.yml` |
| `dev_workspace_name` | `ws-FabricCICD-DEV` | Source workspace (GUIDs → tokens) |
| `generate_parameter_yml` | `True` | Auto-build `parameter.yml` from the repo |
| `rebind_direct_lake` | `True` | Re-point Direct Lake models to the target lakehouse |
| `include_lakehouses` | `True` | Deploy lakehouses from the repo |
| `remove_orphans` | `False` | Delete target items no longer in Git |

## Good to know

- **Table data isn't in Git** — only the lakehouse container. Always run `PL_Refresh_Master` after a deploy.
- **`NB_04_Deploy` publishes the Environment for you** on deploy targets (fabric-cicd builds it and
  waits, ~20 min on first run). Only **Git-synced** workspaces (e.g. Dev) need the manual Environment
  publish described in Part A.
- **Direct Lake on OneLake** can't be rebound by a deployment rule, so `NB_04` does it in code (`semantic-link-labs`; Admin/Member suffices).
- **The `DF_Gold_PA` credential isn't deployed** — follow the one-time source/destination setup in
  Part A for Dev and Part B for each deployment target.
- **Only `NB_04_Deploy` reads Key Vault in the tracked solution.** A private Key Vault requires one
   approved managed private endpoint per Fabric workspace where that notebook executes, plus secret
   read permission for the submitting identity.
- **`parameter.yml` is generated at deploy time**, not checked in — keep `generate_parameter_yml = True`.
- Items pair across stages by **name** — keep display names identical.

## Team development and automatic production deployment

Use `main` as the protected integration and release branch. Do not develop directly in the shared
Dev workspace or commit directly to `main` after the initial repository setup.

### Developer workflow

1. Start from the shared Dev workspace connected to `main`, and make sure it is synchronized from Git.
2. In Fabric source control, use **Branch out to new workspace** to create a short-lived
   `feature/<work-item>` branch and a dedicated feature workspace. One workspace can connect to only
   one branch, so each developer changes and tests items in that isolated workspace.
3. Run the feature workspace end to end. Lakehouse table data and Dataflow credentials aren't copied
   through Git, so bootstrap them as described in Part A when the feature requires executable data.
4. Commit from the feature workspace to its feature branch. Never commit secrets or the insecure
   deployment notebook.
5. Open a pull request from `feature/<work-item>` to `main`. The **Validate Fabric pull request**
   workflow checks Python syntax, JSON item definitions, and required Fabric item files.
6. Require at least one approving review and require the validation check through the GitHub `main`
   branch protection/ruleset. Disable direct pushes to `main`.
7. After the PR merges, delete the feature branch and its temporary Fabric workspace, or clean and
   reuse a developer workspace by reconnecting it to a new branch created from current `main`.
8. Update the shared Dev workspace from Git after merges so the next feature branches from the latest
   integrated definitions.

### GitHub Actions production setup

The [production workflow](.github/workflows/deploy-production.yml) runs on every merge/push to
`main`, so Fabric items can use any valid root folder name without maintaining path filters. It
checks out the approved commit, signs in without a client secret by using GitHub OIDC, installs the
pinned dependencies on the temporary runner, validates the source, and deploys only changed,
missing, or environment-drifted items to Prod. When a commit deletes an item's `.platform` file,
the same run deletes only that matching Prod item; unrelated Fabric-managed items are preserved.
The workflow excludes the entire `Deploy/` subtree and removes any existing deployment notebook
from Prod. `NB_04_Deploy` applies the same rule when it performs a manual deployment.

Configure it once:

1. Create a Microsoft Entra app registration/service principal for GitHub deployment. This repository
   uses the `fabric-rest` application:

   | Identifier | Value |
   | --- | --- |
   | Application (client) ID | `2706a024-95ea-48bd-b789-b4630f13c72d` |
   | Directory (tenant) ID | `ad340c84-1886-4202-a483-2da2cb9168eb` |
   | Service principal object ID | `c1feeecc-0250-4a03-873a-b707fd90e902` |

   Use the application/client ID for GitHub's `AZURE_CLIENT_ID`. Use the service principal object ID
   when adding or checking Fabric workspace role assignments.
2. Add the GitHub OIDC federated credential in **Microsoft Entra ID → App registrations →
   fabric-rest → Certificates & secrets → Federated credentials → Add credential**. The GitHub token
   emitted by this workflow has been verified in the Actions log. Select **Other issuer** and enter:

   | Form field | Value |
   | --- | --- |
   | Federated credential scenario | **Other issuer** |
   | Issuer | `https://token.actions.githubusercontent.com` |
   | Subject identifier | `repo:dibakardharchoudhury/FabricCICD:environment:production` |
   | Audience | `api://AzureADTokenExchange` |
   | Name | `github-fabriccicd-production` |

   The portal's **GitHub Actions deploying Azure resources** scenario now asks for `Organization`,
   `Organization ID`, `Repository`, `Repository ID`, and `Entity type`, then generates an immutable-ID
   subject. Do not use that generated subject for this repository unless GitHub's organization-level
   OIDC subject template is also configured to emit the same immutable-ID claim. With GitHub's current
   default claims, it would not match. The exact subject above is confirmed by the workflow log and
   matches `environment: production`, including case.

   Creating this trust requires an authorized application owner or an Entra **Application
   Administrator**, **Cloud Application Administrator**, or **Global Administrator**. Do not create
   or store a client secret; GitHub exchanges its OIDC token through this credential.
3. In the Fabric Admin portal, enable **Service principals can use Fabric APIs**, preferably scoped to
   a security group containing only this deployment service principal.
4. Add the service principal to both Fabric workspaces under **Manage access**. The least-privilege
   assignments used by this repository are:

   | Workspace | Required role | Reason |
   | --- | --- | --- |
   | `ws-FabricCICD-DEV` | **Viewer** | Resolves source item IDs. |
   | `ws-FabricCICD-PROD` | **Member** | Creates and updates Fabric items, including environment-specific Direct Lake definitions. |

   **Admin** in Prod is also sufficient but is not required for the current deployment workflow.
   Search for `fabric-rest` when adding access and verify that its application ID is
   `2706a024-95ea-48bd-b789-b4630f13c72d`.
5. In GitHub, create the `production` Environment under **Settings → Environments** and add these
   environment variables:

   | Variable | Value |
   | --- | --- |
   | `AZURE_CLIENT_ID` | Entra application/client ID |
   | `AZURE_TENANT_ID` | Entra tenant ID |
   | `FABRIC_DEV_WORKSPACE` | Shared Dev workspace name, for example `ws-FabricCICD-DEV` |
   | `FABRIC_PROD_WORKSPACE` | Production workspace name, for example `ws-FabricCICD-PROD` |

6. Optionally configure required reviewers on the GitHub `production` Environment for a second
   release approval after the PR approval. Without environment reviewers, deployment starts
   automatically as soon as the approved PR merges.

GitHub Actions does **not** run `NB_04`, clone with the Key Vault PAT, or use the Fabric workspace's
Key Vault managed private endpoint. GitHub checks out the repository with its built-in token and the
deployment script authenticates to Fabric through OIDC. `NB_04` remains available for manual,
in-Fabric deployment.

Notebook runtime identity is configured on each activity under **Settings → Connection**; changing
pipeline metadata is not an identity configuration. Microsoft documents
[Notebook activities with service-principal or workspace-identity connections](https://learn.microsoft.com/fabric/data-factory/notebook-activity).
An SPN connection requires a tenant ID, client ID, and service-principal key stored in Fabric. The
current `fabric-rest` app is GitHub OIDC-only and has no key, so that connection cannot be created
without deliberately adding a credential. Do not substitute the pipeline's `LastModifiedBy` value.

### What "only changed items" means

The workflow compares the pushed commit with the push event's previous commit and passes only changed
Fabric item folders to `publish_all_items()`. It also includes repository items missing from Prod, so
the same command can bootstrap newly added items. Semantic models whose deployed connection still
contains the Dev workspace ID are included as environment drift and repaired even when their Git
folder did not change. Use `--full-deploy` only for a deliberate complete bootstrap or recovery.

Environment replacement still runs for every selected item. Notebook default and known lakehouse
IDs, dataflow source/destination IDs, pipeline logical item references, and zero workspace placeholders
resolve to their Prod counterparts. Direct Lake on OneLake models receive both the Prod workspace ID
and matching Prod lakehouse ID in the same semantic-model publish transaction; there is no separate
post-publish XMLA save.

### Verified smoke tests

| Case | Result |
| --- | --- |
| Add `CICD_E2E_Verification.Lakehouse` | [Run 34244155478](https://github.com/dibakardharchoudhury/FabricCICD/actions/runs/34244155478) created it in Prod and published no other item. |
| Change only its `.platform` description | [Run 34244500599](https://github.com/dibakardharchoudhury/FabricCICD/actions/runs/34244500599) updated the same Prod item ID and published no other item. |

Deletion is change-scoped: removing an item folder from Git deletes the target item with the same
type and display name. The workflow does not use broad orphan removal, so Fabric-managed staging
Lakehouses and other workspace-only operational items are not affected.

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
`NB_03` retries ~28 minutes. If it still fails, inspect the run diagnostics before distinguishing
metadata propagation from an access problem. Steady-state runs preserve table identity.

### `NB_03_Aggregate_Gold` fails: `ModuleNotFoundError: No module named 'sempy_labs'`

The `semanticlink` Environment is present but **unpublished**, so `semantic-link-labs` isn't installed
on the Spark pool. This happens when the Environment arrived via **Git sync** (Git brings the
definition but doesn't build it) — typically the Dev workspace. Fix: **`semanticlink` → Publish**
(~10–20 min), then re-run `PL_Refresh_Master`. Re-publish only when libraries change.
*(Deploy targets don't hit this — `NB_04_Deploy` publishes the Environment during deploy.)*
