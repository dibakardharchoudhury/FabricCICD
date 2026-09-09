# Microsoft Fabric CI/CD - Medallion Demo

This repository promotes a Bronze -> Silver -> Gold Microsoft Fabric solution with
[`fabric-cicd`](https://microsoft.github.io/fabric-cicd/). Production deployment has one supported
path: GitHub Actions authenticates through OIDC and runs `scripts/deploy_to_fabric.py`. There is no
deployment notebook, GitHub PAT, client secret, interactive OAuth connection, or checked-in
environment parameter file.

## Data flow

```text
Bronze_LH <- NB_01_Seed_Bronze
    |
Silver_LH <- NB_02_Transform_Silver
    |
Gold_LH   <- NB_03_Aggregate_Gold
    |
Gold_SM (Direct Lake) -> Gold_Dashboard
```

`PL_Refresh_Master` runs three notebook activities in order:

1. `NB_01_Seed_Bronze`
2. `NB_02_Transform_Silver`
3. `NB_03_Aggregate_Gold`

NB03 writes the Gold tables and rebinds `Gold_SM` to the current workspace's `Gold_LH`. Pipeline
runs pass `refresh_semantic_model=True`, so NB03 retains the Semantic Link
`labs.refresh_semantic_model(...)` path. The notebooks run through a dynamically injected
`Notebook.Actions` Workspace Identity connection.

## Repository layout

| Path | Purpose |
| --- | --- |
| `Bronze/` | Bronze Lakehouse and seed notebook |
| `Silver/` | Silver Lakehouse and transform notebook |
| `Gold/` | Gold Lakehouse, aggregate notebook, semantic model, and report |
| `Seed_Data/` | Master refresh pipeline |
| `semanticlink.Environment/` | Pinned Semantic Link runtime libraries |
| `scripts/deploy_to_fabric.py` | Dynamic OIDC deployment implementation |
| `scripts/validate_repository.py` | Source and pipeline contract validation |

Fabric Git stores item definitions, not Lakehouse table data. A new workspace must run the master
pipeline after deployment to create and populate its tables.

## Dynamic deployment

The deployer resolves both workspace IDs by display name at runtime. It then:

1. Provisions or reuses the target workspace identity.
2. Grants that identity Contributor access to the target workspace if it has no existing role.
3. Creates or reuses a `Notebook.Actions` connection backed by that identity.
4. Discovers Fabric item folders from their `.platform` files.
5. Removes generated Python cache files from Fabric item folders.
6. Generates an ephemeral `parameter.yml`.
7. Replaces the current-workspace placeholder with the target workspace ID.
8. Replaces pipeline logical item IDs with target item IDs.
9. Replaces the dedicated notebook-connection sentinel with the target connection ID.
10. Replaces source Lakehouse IDs with target Lakehouse IDs.
11. Publishes changed, missing, or environment-drifted items.
12. Deletes only Fabric items whose `.platform` file was deleted in the compared Git range.
13. Removes `parameter.yml` on process exit, including failed deployments.

The source placeholders have separate meanings and must stay distinct:

| Source value | Meaning |
| --- | --- |
| `00000000-0000-0000-0000-000000000000` | Current workspace |
| `11111111-1111-1111-1111-111111111111` | Notebook Workspace Identity connection |
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
of Fabric APIs. Grant the deployment principal Viewer access to Dev and Member access to established
targets. A completely new target requires Admin access for its first Workspace Identity provisioning;
the deployer skips that admin-only call on later runs.

The target Workspace Identity is the notebook runtime identity; it is separate from the GitHub OIDC
deployment principal. The deployer grants its workspace role, creates the connection, and injects
the connection ID, so a new target does not require manual identity or connection setup.

## Development flow

1. Branch from current `main` into a short-lived feature branch and feature workspace.
2. Make and run the Fabric changes in that workspace.
3. Commit the Fabric item definitions to the feature branch.
4. Open a pull request; `.github/workflows/validate-pr.yml` validates the source.
5. Merge the approved pull request into `main`.
6. Let `.github/workflows/deploy-production.yml` deploy Production.
7. Run `PL_Refresh_Master` in Production and verify all three activities.

Use `--full-deploy` only for a complete bootstrap or recovery. The optional
`--recreate-analytics-items` workflow input deliberately recreates `Gold_SM` and `Gold_Dashboard`;
it is not part of normal deployment.

## Fresh workspace validation

The `semanticlink` Environment must be published before NB03 can import `sempy_labs`. After the
first deployment, run `PL_Refresh_Master` and verify:

| Lakehouse | Tables |
| --- | --- |
| `Bronze_LH.dbo` | `production_raw`, `cost_raw`, `schedule_raw` |
| `Silver_LH.dbo` | `production_conformed`, `cost_conformed`, `schedule_conformed` |
| `Gold_LH.dbo` | `production_daily`, `cost_monthly`, `schedule_summary`, `field_kpi_facts` |

The Workspace Identity path can execute all notebooks and rebind the Direct Lake model. However,
Microsoft's default token service for service-principal-triggered notebooks supports only a subset
of Semantic Link functions, and semantic-model refresh is not in that supported subset. The live
Production run reached NB03 and received `403 Forbidden` from the Power BI refresh API. Microsoft's
documented workaround requires manually authenticating Semantic Link with service-principal
credentials. This repository intentionally does not add a client secret, certificate, Key Vault
dependency, interactive OAuth connection, raw REST refresh, or token wrapping.

## Troubleshooting

### NB03 cannot import `sempy_labs`

Publish the `semanticlink` Environment and wait for its library build to finish. Git synchronization
creates the Environment definition but does not build it.

### Manual NB03 refresh uses the Fabric host for a Power BI path

NB03 intentionally corrects the pinned SemPy client's default URL to `https://api.powerbi.com/`
before importing Labs. Keep that workaround while the pinned runtime requires it.

### Notebook activity reports a connection or identity error

Rerun deployment. It idempotently provisions the target Workspace Identity, creates or reuses the
named Notebook connection, grants Contributor access when needed, and injects the connection ID
into all three activities. Do not edit the pipeline connections manually.

### NB03 Semantic Link refresh returns 403

The Direct Lake rebind has already completed when this error occurs. Semantic-model refresh is not
supported by Semantic Link's default token service in a service-principal-triggered notebook. A
workspace role alone cannot enable that unsupported function. Do not add a secret-based fallback
unless the credential-free design requirement changes.

### First Direct Lake frame reports `0xC14700DF`

New Delta tables can take several minutes to appear in OneLake metadata. NB03 retries the initial
frame. If all retries fail, inspect the NB03 notebook diagnostics.