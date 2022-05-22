using Orleans;
using System.Collections;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
internal abstract class SolidRowGroup<TRow> : ISolidRowGroup<TRow>
{
    protected SolidRowGroup(int id)
    {
        Id = id;
    }

    [Id(1)]
    public int Id { get; }

    public RowGroupState State => RowGroupState.Solid;

    public int Count => 0;

    public abstract RowGroupStats GetStats();

    public abstract IEnumerator<TRow> GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}