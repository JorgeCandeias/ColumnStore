using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
internal class ColumnStoreStats
{
    [Id(1)]
    public int RowCount { get; set; }

    [Id(2)]
    public InnerStoreStats DeltaStoreStats { get; } = new();

    [Id(3)]
    public InnerStoreStats SolidStoreStats { get; } = new();
}