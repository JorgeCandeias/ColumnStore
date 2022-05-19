using Orleans;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
internal class RowGroupStats
{
    [Id(1)]
    public int Id { get; set; }

    [Id(2)]
    public int RowCount { get; set; }

    [Id(3)]
    public ISet<ColumnSegmentStats> ColumnSegmentStats { get; } = new HashSet<ColumnSegmentStats>();
}