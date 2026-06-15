// Dataflow Gen2: DF_Silver_PA
// Purpose: Production Analysis — clean and conform Bronze production → Silver layer
// Source: Bronze_LH.production_raw
// Destination: Silver_LH.production_conformed (Delta table, replace mode)
// ─────────────────────────────────────────────────────────────────────────────

section DF_Silver_PA;

// ── Source: Bronze production_raw ────────────────────────────────────────────
shared BronzeProduction = let
    LH     = Lakehouse.Contents(null),   // null = default lakehouse in workspace
    Bronze = LH{[workspaceId = null, lakehouseId = null, Id = "Tables"]}[Data],
    Table  = Bronze{[Name = "production_raw"]}[Data]
in
    Table;

// ── Transformation: Clean, filter, compute KPIs ─────────────────────────────
shared SilverProduction = let
    Source = BronzeProduction,

    // Filter out shut-in wells
    ActiveOnly = Table.SelectRows(Source, each [status] <> "Shut-in"),

    // Ensure correct types
    Typed = Table.TransformColumnTypes(ActiveOnly, {
        {"well_id",   type text},
        {"date",      type date},
        {"field",     type text},
        {"oil_bbl",   type number},
        {"gas_mcf",   type number},
        {"water_bbl", type number},
        {"status",    type text}
    }),

    // Compute BOE (Barrels of Oil Equivalent)
    WithBOE = Table.AddColumn(Typed, "boe_total",
        each Number.Round([oil_bbl] + [gas_mcf] * 0.17, 2),
        type number
    ),

    // Compute Water Cut %
    WithWaterCut = Table.AddColumn(WithBOE, "water_cut_pct",
        each if ([oil_bbl] + [water_bbl]) > 0
             then Number.Round(([water_bbl] / ([oil_bbl] + [water_bbl])) * 100, 2)
             else 0.0,
        type number
    ),

    // Add active flag
    WithActive = Table.AddColumn(WithWaterCut, "is_active",
        each [status] = "Active",
        type logical
    ),

    // Add transformation timestamp
    WithTimestamp = Table.AddColumn(WithActive, "transformed_at",
        each DateTime.LocalNow(), type datetime
    ),

    // Remove Bronze metadata columns
    Final = Table.RemoveColumns(WithTimestamp, {"ingested_at", "source_system"})
in
    Final;

// ── Destination: Silver_LH.production_conformed ──────────────────────────────
// Dataflow Gen2 UI → Add data destination → Lakehouse → Silver_LH
// Table: production_conformed | Update method: Replace
