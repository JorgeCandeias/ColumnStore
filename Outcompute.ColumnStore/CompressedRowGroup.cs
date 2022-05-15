using System.Collections;

namespace Outcompute.ColumnStore;

internal abstract class CompressedRowGroup<TRow> : IReadOnlyCollection<TRow>
{
    public int Count { get; protected set; }

    public abstract void Add(TRow row);

    public abstract IEnumerator<TRow> GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public abstract RowGroupStats GetStats();
}