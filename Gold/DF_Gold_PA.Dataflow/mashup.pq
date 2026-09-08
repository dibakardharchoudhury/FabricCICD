[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared SilverWorkspaceId = "292e18c3-b95e-42d1-bb02-9a2064fee5b8" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared SilverLakehouseId = "4207398e-6d15-4407-8d8d-cbb50d661ec6" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
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
  Navigation_1 = Pattern{[workspaceId = "292e18c3-b95e-42d1-bb02-9a2064fee5b8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "06434335-fbfd-4d2b-879d-4962bdd8d605"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "production_daily", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
