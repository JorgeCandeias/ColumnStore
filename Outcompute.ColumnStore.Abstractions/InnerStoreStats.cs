using Orleans;
using System.Collections.Immutable;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public class InnerStoreStats
{
    [Id(1)]
    public int RowCount { get; init; }

    [Id(2)]
    public IReadOnlyDictionary<int, RowGroupStats> RowGroupStats { get; init; } = ImmutableDictionary<int, RowGroupStats>.Empty;

    public class Builder
    {
        internal Builder()
        {
        }

        public int RowCount { get; set; }

        public ImmutableDictionary<int, RowGroupStats>.Builder RowGroupStats { get; } = ImmutableDictionary.CreateBuilder<int, RowGroupStats>();

        public InnerStoreStats ToImmutable() => new()
        {
            RowCount = RowCount,
            RowGroupStats = RowGroupStats.ToImmutable()
        };
    }

    public static Builder CreateBuilder() => new();

    public static InnerStoreStats Empty { get; } = new()
    {
        RowCount = 0,
        RowGroupStats = ImmutableDictionary<int, RowGroupStats>.Empty
    };
}