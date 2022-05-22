using Orleans;
using System.Collections;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
public abstract class SolidRowGroup<TRow> : ISolidRowGroup<TRow>
{
    private readonly RowGroupStats _stats;

    protected SolidRowGroup(RowGroupStats stats)
    {
        Guard.IsNotNull(stats, nameof(stats));

        _stats = stats;
    }

    [Id(1)]
    public int Id { get; }

    public RowGroupState State => RowGroupState.Solid;

    public int Count => _stats.RowCount;

    public RowGroupStats GetStats() => _stats;

    public abstract IEnumerator<TRow> GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}