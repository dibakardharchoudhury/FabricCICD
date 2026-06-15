// Dataflow Gen2: DF_Bronze_Production
// Purpose: Ingest production data from SharePoint/OneLake Files into Bronze_LH.production_raw
// Source: SharePoint Excel file (simulates PIMS source)
// Destination: Bronze_LH.production_raw (Delta table, append mode)
// ─────────────────────────────────────────────────────────────────────────────

section DF_Bronze_Production;

// ── Source: SharePoint Excel (simulates PIMS) ────────────────────────────────
shared ProductionSource = let
    // In real deployment: replace with actual SharePoint connection
    Source = SharePoint.Tables(
        "https://yourorg.sharepoint.com/sites/DataIngestion",
        [ApiVersion = 15]
    ),
    ProductionList = Source{[Title="ProductionData"]}[Items],
    // OR use OneLake Files path for the demo:
    // Source = Lakehouse.Contents("Bronze_LH"),
    // ProductionFile = Source{[Id="Files"]}[Data],
    // CSVFile = ProductionFile{[Name="production_data.csv"]}[Content],

    // Type the columns
    TypedTable = Table.TransformColumnTypes(ProductionList, {
        {"well_id",   type text},
        {"date",      type date},
        {"field",     type text},
        {"oil_bbl",   type number},
        {"gas_mcf",   type number},
        {"water_bbl", type number},
        {"status",    type text}
    }),

    // Add ingestion metadata
    WithTimestamp = Table.AddColumn(TypedTable, "ingested_at", each DateTime.LocalNow(), type datetime),
    WithSource    = Table.AddColumn(WithTimestamp, "source_system", each "PIMS", type text),

    // Remove SharePoint system columns
    FinalTable = Table.SelectColumns(WithSource, {
        "well_id", "date", "field", "oil_bbl", "gas_mcf", "water_bbl",
        "status", "ingested_at", "source_system"
    })
in
    FinalTable;

// ── Destination: Bronze_LH.production_raw ────────────────────────────────────
// Configure in the Dataflow Gen2 UI:
//   Destination → Lakehouse → Bronze_LH → Table: production_raw
//   Update method: Append (for incremental loads) or Replace (for full refresh)
