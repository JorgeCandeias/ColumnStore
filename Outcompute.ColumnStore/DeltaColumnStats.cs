using Orleans;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
internal class DeltaColumnStats
{
    public DeltaColumnStats(string name)
    {
        Name = name;
    }

    [Id(1)]
    public string Name { get; }

    [Id(2)]
    public int DistinctValueCount { get; set; }
}