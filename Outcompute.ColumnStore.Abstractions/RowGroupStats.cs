﻿using Orleans;
using System.Collections.Immutable;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public record RowGroupStats(
    [property: Id(1)] int Id,
    [property: Id(2)] int RowCount,
    [property: Id(3)] IReadOnlyDictionary<string, ColumnSegmentStats> ColumnSegmentStats)
{
    public class Builder
    {
        internal Builder()
        {
        }

        public int Id { get; set; }

        public int RowCount { get; set; }

        public ImmutableDictionary<string, ColumnSegmentStats>.Builder ColumnSegmentStats { get; } = ImmutableDictionary.CreateBuilder<string, ColumnSegmentStats>();

        public RowGroupStats ToImmutable() => new(Id, RowCount, ColumnSegmentStats.ToImmutable());
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
}