using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
internal class InnerStoreStats
{
    [Id(1)]
    public int RowCount { get; set; }

    [Id(2)]
    public ISet<RowGroupStats> RowGroupStats { get; } = new HashSet<RowGroupStats>();
}