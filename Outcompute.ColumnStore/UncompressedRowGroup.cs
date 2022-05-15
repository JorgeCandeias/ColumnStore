using System.Collections;

namespace Outcompute.ColumnStore;

public class UncompressedRowGroup<TRow> : IReadOnlyList<TRow>
{
    private readonly ColumnStoreOptions _options;

    public UncompressedRowGroup(ColumnStoreOptions options)
    {
        Guard.IsNotNull(options, nameof(options));

        _options = options;
    }

    private readonly List<TRow> _rows = new();

    public int Count => _rows.Count;

    public TRow this[int index] => _rows[index];

    public RowGroupState State { get; private set; } = RowGroupState.Open;

    public void Add(TRow item)
    {
        if (State != RowGroupState.Open)
        {
            ThrowHelper.ThrowInvalidOperationException($"This {nameof(UncompressedRowGroup<TRow>)} is closed");
        }

        _rows.Add(item);

        if (_rows.Count >= _options.RowGroupSizeThreshold)
        {
            State = RowGroupState.Closed;
        }
    }

    public IEnumerator<TRow> GetEnumerator() => _rows.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}