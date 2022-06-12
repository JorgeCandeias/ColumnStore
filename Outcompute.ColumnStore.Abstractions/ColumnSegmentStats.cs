using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public sealed record ColumnSegmentStats(
    [property: Id(1)] string Name,
    [property: Id(2)] int DistinctValueCount,
    [property: Id(3)] int DefaultValueCount)
{
    public class Builder
    {
        internal Builder()
        {
        }

        public string Name { get; set; } = "";

        public int DistinctValueCount { get; set; }

        public int DefaultValueCount { get; set; }

        public ColumnSegmentStats ToImmutable() => new(Name, DistinctValueCount, DefaultValueCount);
    }

    public static Builder CreateBuilder() => new();
}