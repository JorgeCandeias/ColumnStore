using Orleans;
using static System.String;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public class DeltaColumnStats
{
    [Id(1)]
    public string Name { get; init; } = Empty;

    [Id(2)]
    public int DistinctValueCount { get; init; }

    public class Builder
    {
        internal Builder()
        { }

        public string Name { get; set; } = "";

        public int DistinctValueCount { get; set; }

        public DeltaColumnStats ToImmutable() => new()
        {
            Name = Name,
            DistinctValueCount = DistinctValueCount,
        };
    }

    public static Builder CreateBuilder() => new();
}