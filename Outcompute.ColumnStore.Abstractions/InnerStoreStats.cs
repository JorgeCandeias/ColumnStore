using Orleans;
using System.Collections.Immutable;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public record InnerStoreStats(
    [property: Id(1)] int RowCount,
    [property: Id(2)] IReadOnlyDictionary<int, IRowGroupStats> RowGroupStats)
{
    public class Builder
    {
        internal Builder()
        {
        }

        public int RowCount { get; set; }

        public ImmutableDictionary<int, IRowGroupStats>.Builder RowGroupStats { get; } = ImmutableDictionary.CreateBuilder<int, IRowGroupStats>();

        public InnerStoreStats ToImmutable() => new(RowCount, RowGroupStats.ToImmutable());
    }

    public static Builder CreateBuilder() => new();

    public static InnerStoreStats Empty { get; } = new(0, ImmutableDictionary<int, IRowGroupStats>.Empty);
}