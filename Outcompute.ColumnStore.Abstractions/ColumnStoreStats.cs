using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public record ColumnStoreStats(
    [property: Id(1)] int RowCount,
    [property: Id(2)] InnerStoreStats DeltaStoreStats,
    [property: Id(3)] InnerStoreStats SolidStoreStats)
{
    public class Builder
    {
        internal Builder()
        {
        }

        public int RowCount { get; set; }

        public InnerStoreStats DeltaStoreStats { get; set; } = InnerStoreStats.Empty;

        public InnerStoreStats.Builder SolidStoreStats { get; } = InnerStoreStats.CreateBuilder();

        public ColumnStoreStats ToImmutable() => new(RowCount, DeltaStoreStats, SolidStoreStats.ToImmutable());
    }

    public static Builder CreateBuilder() => new();
}