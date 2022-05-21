using Orleans;
using System.Collections.Immutable;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public record InnerStoreStats(
    [property: Id(1)] int RowCount,
    [property: Id(2)] IReadOnlyDictionary<int, RowGroupStats> RowGroupStats)
{
    public class Builder
    {
        internal Builder()
        {
        }

        public int RowCount { get; set; }

        public ImmutableDictionary<int, RowGroupStats>.Builder RowGroupStats { get; } = ImmutableDictionary.CreateBuilder<int, RowGroupStats>();

        public InnerStoreStats ToImmutable() => new(RowCount, RowGroupStats.ToImmutable());
    }

    public static Builder CreateBuilder() => new();

    public static InnerStoreStats Empty { get; } = new(0, ImmutableDictionary<int, RowGroupStats>.Empty);
}