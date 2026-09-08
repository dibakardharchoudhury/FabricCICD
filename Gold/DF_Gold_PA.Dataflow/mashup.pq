[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared SilverWorkspaceId = "3f0dbdcc-2520-4432-bbb2-c7a53375c73c" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared SilverLakehouseId = "9c0b5838-532b-4a99-9d34-29cd1576d7a7" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared SilverProduction = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Workspace = Source{[workspaceId = SilverWorkspaceId]}[Data],
  Lake = Workspace{[lakehouseId = SilverLakehouseId]}[Data],
  Table = Lake{[Id = "production_conformed", ItemKind = "Table"]}[Data]
in
  Table;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "GoldProductionDaily_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared GoldProductionDaily = let
  Source = SilverProduction,
  Grouped = Table.Group(Source, {"date", "field"}, {
    {"total_oil_bbl", each List.Sum([oil_bbl]), type number},
    {"total_gas_mcf", each List.Sum([gas_mcf]), type number},
    {"total_water_bbl", each List.Sum([water_bbl]), type number},
    {"total_boe", each List.Sum([boe_total]), type number},
    {"avg_water_cut_pct", each List.Average([water_cut_pct]), type number},
    {"active_well_count", each Table.RowCount(_), type number}
  }),
  Rounded = Table.TransformColumns(Grouped, {
    {"total_oil_bbl", each Number.Round(_, 2), type number},
    {"total_gas_mcf", each Number.Round(_, 2), type number},
    {"total_boe", each Number.Round(_, 2), type number},
    {"avg_water_cut_pct", each Number.Round(_, 2), type number}
  }),
  WithTimestamp = Table.AddColumn(Rounded, "report_generated_at", each DateTime.LocalNow(), type datetime),
  Sorted = Table.Sort(WithTimestamp, {{"date", Order.Ascending}, {"field", Order.Ascending}})
in
  Sorted;
shared GoldProductionDaily_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "3f0dbdcc-2520-4432-bbb2-c7a53375c73c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "663f3fff-65c9-485e-ae23-fd03018fc613"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "production_daily", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
