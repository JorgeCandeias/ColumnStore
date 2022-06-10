using Orleans;
using System.Collections.Immutable;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public class RowGroupStats : IRowGroupStats
{
    private RowGroupStats()
    {
    }

    [Id(1)]
    public int Id { get; init; }

    [Id(2)]
    public int RowCount { get; init; }

    [Id(3)]
    public IReadOnlyDictionary<string, IColumnSegmentStats> ColumnSegmentStats { get; init; } = null!;

    public class Builder
    {
        internal Builder()
        {
        }

        public int Id { get; set; }

        public int RowCount { get; set; }

        public ImmutableDictionary<string, IColumnSegmentStats>.Builder ColumnSegmentStats { get; } = ImmutableDictionary.CreateBuilder<string, IColumnSegmentStats>();

        public RowGroupStats ToImmutable() => new()
        {
            Id = Id,
            RowCount = RowCount,
            ColumnSegmentStats = ColumnSegmentStats.ToImmutable()
        };
    }

    public Builder ToBuilder()
    {
        var builder = CreateBuilder();

        builder.Id = Id;
        builder.RowCount = RowCount;

        foreach (var item in ColumnSegmentStats)
        {
            builder.ColumnSegmentStats[item.Key] = item.Value;
        }

        return builder;
    }

    public static Builder CreateBuilder() => new();

    public static RowGroupStats Empty { get; } = new()
    {
        Id = 0,
        RowCount = 0,
        ColumnSegmentStats = ImmutableDictionary<string, IColumnSegmentStats>.Empty
    };
}