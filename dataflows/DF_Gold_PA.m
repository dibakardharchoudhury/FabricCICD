// Dataflow Gen2: DF_Gold_PA
// Purpose: Production Analysis aggregation — Silver production → Gold daily summary
// Source: Silver_LH.production_conformed
// Destination: Gold_LH.production_daily (Delta table, replace mode)
// ─────────────────────────────────────────────────────────────────────────────

section DF_Gold_PA;

// ── Parameters: Silver lakehouse identity (the READ source) ───────────────────
// These two identify the SOURCE lakehouse you read FROM (Silver_LH), NOT the Gold
// destination. The Gold_LH destination is configured separately via the dataflow's
// "Add data destination" UI on the GoldProductionDaily query (it writes its own hidden
// GoldProductionDaily_DataDestination query with the Gold IDs).
// Replace these placeholder GUIDs with YOUR Silver_LH IDs — find them in the lakehouse
// URL when you open Silver_LH in Fabric: .../groups/<SilverWorkspaceId>/lakehouses/<SilverLakehouseId>
shared SilverWorkspaceId = "00000000-0000-0000-0000-000000000000" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared SilverLakehouseId = "00000000-0000-0000-0000-000000000000" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];

// ── Source: Silver production_conformed ──────────────────────────────────────
shared SilverProduction = let
    Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Workspace = Source{[workspaceId = SilverWorkspaceId]}[Data],
    Lake = Workspace{[lakehouseId = SilverLakehouseId]}[Data],
    Table = Lake{[Id = "production_conformed", ItemKind = "Table"]}[Data]
in
    Table;

// ── Aggregation: Daily field-level production summary ────────────────────────
shared GoldProductionDaily = let
    Source = SilverProduction,

    // Group by date + field
    Grouped = Table.Group(
        Source,
        {"date", "field"},
        {
            {"total_oil_bbl",       each List.Sum([oil_bbl]),       type number},
            {"total_gas_mcf",       each List.Sum([gas_mcf]),       type number},
            {"total_water_bbl",     each List.Sum([water_bbl]),     type number},
            {"total_boe",           each List.Sum([boe_total]),     type number},
            {"avg_water_cut_pct",   each List.Average([water_cut_pct]), type number},
            {"active_well_count",   each Table.RowCount(_),         type number}
        }
    ),

    // Round aggregates
    Rounded = Table.TransformColumns(Grouped, {
        {"total_oil_bbl",       each Number.Round(_, 2), type number},
        {"total_gas_mcf",       each Number.Round(_, 2), type number},
        {"total_boe",           each Number.Round(_, 2), type number},
        {"avg_water_cut_pct",   each Number.Round(_, 2), type number}
    }),

    // Add report generation timestamp
    WithTimestamp = Table.AddColumn(Rounded, "report_generated_at",
        each DateTime.LocalNow(), type datetime
    ),

    // Sort for readability
    Sorted = Table.Sort(WithTimestamp, {{"date", Order.Ascending}, {"field", Order.Ascending}})
in
    Sorted;

// ── Destination: Gold_LH.production_daily ────────────────────────────────────
// Dataflow Gen2 UI → Add data destination → Lakehouse → Gold_LH
// Table: production_daily | Update method: Replace
