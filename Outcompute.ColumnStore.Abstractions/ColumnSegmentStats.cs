using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
internal class ColumnSegmentStats
{
    [Id(1)]
    public string ColumnName { get; set; } = "";

    [Id(2)]
    public int RowCount { get; set; }

    [Id(3)]
    public int RangeCount { get; set; }

    [Id(4)]
    public int DistinctValueCount { get; set; }

    [Id(5)]
    public int DefaultValueCount { get; set; }
}