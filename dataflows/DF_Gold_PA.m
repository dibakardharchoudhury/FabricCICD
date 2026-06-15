// Dataflow Gen2: DF_Gold_PA
// Purpose: Production Analysis aggregation — Silver production → Gold daily summary
// Source: Silver_LH.production_conformed
// Destination: Gold_LH.production_daily (Delta table, replace mode)
// ─────────────────────────────────────────────────────────────────────────────

section DF_Gold_PA;

// ── Source: Silver production_conformed ──────────────────────────────────────
shared SilverProduction = let
    LH     = Lakehouse.Contents(null),
    Silver = LH{[workspaceId = null, lakehouseId = null, Id = "Tables"]}[Data],
    Table  = Silver{[Name = "production_conformed"]}[Data]
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
