using System.Collections;

namespace Outcompute.ColumnStore;

internal class DeltaStore<TRow> : IReadOnlyCollection<TRow>
{
    private readonly ColumnStoreOptions _options;

    public DeltaStore(ColumnStoreOptions options)
    {
        Guard.IsNotNull(options, nameof(options));

        _options = options;
        //_active = new UncompressedRowGroup<TRow>(options);
        _groups.Add(_active);
    }

    private readonly HashSet<DeltaRowGroup<TRow>> _groups = new();

    private DeltaRowGroup<TRow> _active;

    public int Count { get; private set; }

    public void Add(TRow item)
    {
        _active.Add(item);

        if (_active.State == RowGroupState.Closed)
        {
            //_active = new(_options);
            _groups.Add(_active);
        }

        Count++;
    }

    public bool TryTakeClosed(out DeltaRowGroup<TRow> group)
    {
        if (_groups.Count > 1)
        {
            foreach (var item in _groups)
            {
                if (item.State == RowGroupState.Closed)
                {
                    _groups.Remove(item);
                    Count -= item.Count;

                    group = item;
                    return true;
                }
            }
        }

        group = default!;
        return false;
    }

    public IEnumerator<TRow> GetEnumerator()
    {
        foreach (var group in _groups)
        {
            foreach (var row in group)
            {
                yield return row;
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}