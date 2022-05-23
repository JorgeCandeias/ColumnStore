using Orleans;
using System.Collections;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
public abstract class SolidRowGroup<TRow> : ISolidRowGroup<TRow>
{
    protected SolidRowGroup(int id, IRowGroupStats stats)
    {
        Guard.IsNotNull(stats, nameof(stats));

        Id = id;
        Stats = stats;
    }

    [Id(1)]
    public int Id { get; }

    [Id(2)]
    public RowGroupState State => RowGroupState.Solid;

    [Id(3)]
    public IRowGroupStats Stats { get; }

    public int Count => Stats.RowCount;

    public abstract IEnumerator<TRow> GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}