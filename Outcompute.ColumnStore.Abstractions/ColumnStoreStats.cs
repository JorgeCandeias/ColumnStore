using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public class ColumnStoreStats
{
    [Id(1)]
    public int RowCount { get; init; }

    [Id(2)]
    public InnerStoreStats DeltaStoreStats { get; init; } = InnerStoreStats.Empty;

    [Id(3)]
    public InnerStoreStats SolidStoreStats { get; init; } = InnerStoreStats.Empty;

    public class Builder
    {
        internal Builder()
        {
        }

        public int RowCount { get; set; }

        public InnerStoreStats DeltaStoreStats { get; set; } = InnerStoreStats.Empty;

        public InnerStoreStats SolidStoreStats { get; set; } = InnerStoreStats.Empty;

        public ColumnStoreStats ToImmutable() => new()
        {
            RowCount = RowCount,
            DeltaStoreStats = DeltaStoreStats,
            SolidStoreStats = SolidStoreStats
        };
    }

    public static Builder CreateBuilder() => new();
}