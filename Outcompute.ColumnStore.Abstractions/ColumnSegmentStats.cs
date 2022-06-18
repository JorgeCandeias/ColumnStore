using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public sealed record ColumnSegmentStats(
    [property: Id(1)] string Name,
    [property: Id(2)] int RowCount,
    [property: Id(3)] int RangeCount,
    [property: Id(4)] int DistinctValueCount,
    [property: Id(5)] int DefaultValueCount)
{
    public class Builder
    {
        internal Builder()
        {
        }

        public string Name { get; set; } = "";

        public int RowCount { get; set; } = 0;

        public int RangeCount { get; set; } = 0;

        public int DistinctValueCount { get; set; }

        public int DefaultValueCount { get; set; }

        public ColumnSegmentStats ToImmutable() => new(Name, RowCount, RangeCount, DistinctValueCount, DefaultValueCount);
    }

    public static Builder CreateBuilder() => new();
}