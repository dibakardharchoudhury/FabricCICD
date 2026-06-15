<#
.SYNOPSIS
    Packages each pipeline JSON in /pipelines into an importable .zip template.

.DESCRIPTION
    The Fabric Data Factory "Import" dialog (pipeline editor -> Home -> Import)
    accepts a .zip, not a raw pipeline .json. The .zip mirrors Fabric's
    documented Git-integration item format for a Data Pipeline:

        <PipelineName>.zip
          |- pipeline-content.json   # the pipeline "properties" payload
          |- .platform               # item metadata (type/displayName/description)

    This script reads every *.json in /pipelines, wraps it into that two-file
    layout, and zips it next to the source as <PipelineName>.zip.

    NOTE (read before importing):
      * The Import dialog expects connections to be re-mapped after import.
        These demo pipelines reference Lakehouse datasets/connections by name
        (ADF/Synapse style); on import you will be prompted to bind them to the
        connections in your target workspace.
      * The guaranteed, fully supported way to land these items in a Fabric
        workspace is Git integration (sync the repo) or rebuilding on the
        canvas. The .zip is provided as a convenience for the Import button.

.EXAMPLE
    pwsh ./scripts/04_build_pipeline_templates.ps1
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

$repoRoot    = Split-Path -Parent $PSScriptRoot
$pipelineDir = Join-Path $repoRoot 'pipelines'
$buildRoot   = Join-Path ([System.IO.Path]::GetTempPath()) 'fabric_pipeline_build'

if (-not (Test-Path $pipelineDir)) {
    throw "Pipelines folder not found: $pipelineDir"
}

if (Test-Path $buildRoot) { Remove-Item $buildRoot -Recurse -Force }
New-Item -ItemType Directory -Path $buildRoot -Force | Out-Null

$built = @()

Get-ChildItem -Path $pipelineDir -Filter '*.json' | ForEach-Object {
    $sourcePath = $_.FullName
    $pipeline   = Get-Content $sourcePath -Raw | ConvertFrom-Json

    if (-not $pipeline.name -or -not $pipeline.properties) {
        Write-Warning "Skipping $($_.Name): missing 'name' or 'properties'."
        return
    }

    $name        = $pipeline.name
    $description = if ($pipeline.properties.description) { $pipeline.properties.description } else { '' }

    # --- staging folder ---
    $stage = Join-Path $buildRoot $name
    New-Item -ItemType Directory -Path $stage -Force | Out-Null

    # --- pipeline-content.json (the importable pipeline payload) ---
    $content = [ordered]@{ properties = $pipeline.properties }
    $content | ConvertTo-Json -Depth 100 |
        Set-Content -Path (Join-Path $stage 'pipeline-content.json') -Encoding UTF8

    # --- .platform (Fabric item metadata) ---
    $platform = [ordered]@{
        '$schema' = 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json'
        metadata  = [ordered]@{
            type        = 'DataPipeline'
            displayName = $name
            description = $description
        }
        config    = [ordered]@{
            version   = '2.0'
            logicalId = '00000000-0000-0000-0000-000000000000'
        }
    }
    $platform | ConvertTo-Json -Depth 20 |
        Set-Content -Path (Join-Path $stage '.platform') -Encoding UTF8

    # --- zip (explicit file list so the dot-prefixed .platform is included) ---
    $zipPath = Join-Path $pipelineDir "$name.zip"
    if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

    Compress-Archive `
        -Path (Join-Path $stage 'pipeline-content.json'), (Join-Path $stage '.platform') `
        -DestinationPath $zipPath -Force

    $built += $zipPath
    Write-Host "Built $([System.IO.Path]::GetFileName($zipPath))"
}

Remove-Item $buildRoot -Recurse -Force

Write-Host ""
Write-Host "Done. $($built.Count) template(s) written to /pipelines:"
$built | ForEach-Object { Write-Host "  - $([System.IO.Path]::GetFileName($_))" }
