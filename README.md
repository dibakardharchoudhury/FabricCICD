# Microsoft Fabric CI/CD - Medallion Demo

This repository promotes a Bronze -> Silver -> Gold Microsoft Fabric solution with
[`fabric-cicd`](https://microsoft.github.io/fabric-cicd/). GitHub Actions deploys normal items with
OIDC. The refresh notebook and its pipeline are published separately by the designated Entra user,
without a GitHub PAT, client secret, or checked-in environment parameter file.

## Data flow

```text
Bronze_LH <- NB_01_Seed_Bronze
    |
Silver_LH <- NB_02_Transform_Silver
    |
Gold_LH   <- NB_03_Aggregate_Gold
    |
NB_04_SemanticModelReBindRefresh -> Gold_SM (Direct Lake) -> Gold_Dashboard
```

`PL_Refresh_Master` runs three notebook activities in order:

1. `NB_01_Seed_Bronze`
2. `NB_02_Transform_Silver`
3. `NB_03_Aggregate_Gold`

NB03 writes the Gold tables. `PL_SemanticModel_Rebind_Refresh` then runs NB04 to validate the Gold
tables, rebind `Gold_SM` to the current workspace's `Gold_LH`, and perform a full model refresh. Both
pipeline definitions are grouped under `Pipelines/`; the older native `PL_Refresh_SemanticModel`
pipeline has been removed.

## Repository layout

| Path | Purpose |
| --- | --- |
| `Bronze/` | Bronze Lakehouse and seed notebook |
| `Silver/` | Silver Lakehouse and transform notebook |
| `Gold/` | Gold Lakehouse, aggregate and refresh notebooks, semantic model, and report |
| `Pipelines/` | Master data pipeline and administrator-owned semantic model refresh pipeline |
| `semanticlink.Environment/` | Pinned Semantic Link runtime libraries |
| `scripts/deploy_to_fabric.py` | Dynamic OIDC deployment implementation |
| `scripts/validate_repository.py` | Source and pipeline contract validation |

Fabric Git stores item definitions, not Lakehouse table data. A new workspace must run the master
pipeline after deployment to create and populate its tables.

## Dynamic deployment

The deployer resolves both workspace IDs by display name at runtime. It then:

1. Discovers Fabric item folders from their `.platform` files.
2. Removes generated Python cache files from Fabric item folders.
3. Generates an ephemeral `parameter.yml`.
4. Replaces the current-workspace placeholder with the target workspace ID.
5. Replaces pipeline logical item IDs with target item IDs.
6. Replaces source Lakehouse IDs with target Lakehouse IDs.
7. Publishes changed, missing, or environment-drifted items.
8. Deletes only Fabric items whose `.platform` file was deleted in the compared Git range.
9. Removes `parameter.yml` on process exit, including failed deployments.

The normal OIDC path excludes `NB_04_SemanticModelReBindRefresh` and
`PL_SemanticModel_Rebind_Refresh` from publication and deletion so the service principal cannot
replace their publisher. Publish only those items from an Azure CLI session authenticated as
`admin@mngenvmcap218279.onmicrosoft.com`:

```powershell
.venv\Scripts\python.exe scripts\deploy_to_fabric.py `
    --target-workspace ws-FabricCICD-PROD `
    --dev-workspace ws-FabricCICD-DEV `
    --environment Production `
    --repository-directory . `
    --admin-owned-only
```

The command verifies both the user's Entra UPN and object ID before publishing. It publishes only
NB04 and `PL_SemanticModel_Rebind_Refresh`.

The source references have separate meanings:

| Source value | Meaning |
| --- | --- |
| `00000000-0000-0000-0000-000000000000` | Current workspace |
| `.platform` `config.logicalId` | Pipeline item reference |

Do not replace these with live Production IDs. Runtime IDs belong only in the generated,
untracked `parameter.yml`.

## GitHub configuration

The [production workflow](.github/workflows/deploy-production.yml) runs on pushes to `main` and can
also be dispatched manually. Configure a GitHub `production` Environment with:

| Variable | Purpose |
| --- | --- |
| `AZURE_CLIENT_ID` | Entra application used for GitHub OIDC |
| `AZURE_TENANT_ID` | Entra tenant containing the Fabric workspaces |
| `FABRIC_DEV_WORKSPACE` | Source workspace display name |
| `FABRIC_PROD_WORKSPACE` | Target workspace display name |

Create an Entra federated credential for the repository's GitHub `production` Environment with
audience `api://AzureADTokenExchange`. Do not create a client secret. Enable service-principal use
of Fabric APIs and grant the deployment principal Viewer access to Dev and Member access to Prod.
The pipeline notebook activities use Fabric's normal execution context and have no external
Notebook connection.

## Development flow

1. Branch from current `main` into a short-lived feature branch and feature workspace.
2. Make and run the Fabric changes in that workspace.
3. Commit the Fabric item definitions to the feature branch.
4. Open a pull request; `.github/workflows/validate-pr.yml` validates the source.
5. Merge the approved pull request into `main`.
6. Let `.github/workflows/deploy-production.yml` deploy Production.
7. Run `PL_Refresh_Master` in Production and verify all three activities.
8. Publish the two protected refresh items with `--admin-owned-only`, then run
    `PL_SemanticModel_Rebind_Refresh` and verify it completes.

Use `--full-deploy` only for a complete bootstrap or recovery.

## Fresh workspace validation

The `semanticlink` Environment must be published before NB04 can import `sempy_labs`. After the
first deployment, run `PL_Refresh_Master` and verify:

| Lakehouse | Tables |
| --- | --- |
| `Bronze_LH.dbo` | `production_raw`, `cost_raw`, `schedule_raw` |
| `Silver_LH.dbo` | `production_conformed`, `cost_conformed`, `schedule_conformed` |
| `Gold_LH.dbo` | `production_daily`, `cost_monthly`, `schedule_summary`, `field_kpi_facts` |

After the master pipeline succeeds, run `PL_SemanticModel_Rebind_Refresh`. NB04 uses the same
dynamic `Gold_LH` and `semanticlink` dependencies as NB03, rebinds the model, and retries the
observed transient Direct Lake framing failure before reporting success.

### Manual refresh fallback

1. Open the target Fabric workspace.
2. After `PL_Refresh_Master` and NB03 complete, wait about two minutes before refreshing the Direct
    Lake model.
3. Find `Gold_SM` and select its **Refresh** button, or open its context menu and select
    **Refresh now**.
4. Open the semantic model refresh history and confirm the refresh completed before validating
    `Gold_Dashboard`.

If an immediate refresh says that `field_kpi_facts` does not exist or access was denied even though
the table is present in `Gold_LH.dbo`, allow more time and retry. This exact framing error has been
observed to clear without permission or ownership changes; do not infer its internal cause without
detailed refresh diagnostics.

### Scheduled refresh alternative

If the dedicated refresh pipeline is intentionally disabled, Production can use the semantic
model's own refresh schedule:

1. Open `Gold_SM` in the Production workspace and select **Settings**.
2. Open **Refresh** or **Scheduled refresh**, enable the schedule, and set the time zone and desired
    refresh times.
3. Schedule the model after `PL_Refresh_Master` normally finishes, including at least a two-minute
    buffer in addition to notebook runtime and first-run table creation.
4. Save the schedule and monitor both pipeline history and semantic model refresh history separately.

Do not run this schedule at the same time as `PL_SemanticModel_Rebind_Refresh`.

## Troubleshooting

### NB04 cannot import `sempy_labs`

Publish the `semanticlink` Environment and wait for its library build to finish. Git synchronization
creates the Environment definition but does not build it.

### NB04 Git sync reports `PyToIpynbFailure`

Fabric notebook source is line-delimited. Ensure `notebook-content.py` ends with a newline after its
final `# META }` record, then rerun `python scripts/validate_repository.py` before syncing. The
validator rejects missing notebook terminators so this malformed source cannot be committed again.

### Gold_SM is not current after the master pipeline

NB03 only writes Gold tables. Run `PL_SemanticModel_Rebind_Refresh`, then check its run history and
the semantic model refresh history.

### First Direct Lake frame reports `0xC14700DF`

If a manual or scheduled refresh fails immediately after deployment, table creation, or Direct Lake
rebinding, wait at least two minutes and retry from semantic model refresh history. Later success
without permission changes establishes that the observed case is transient, but does not establish
whether the internal cause is metadata synchronization, framing state, or another Fabric behavior.
