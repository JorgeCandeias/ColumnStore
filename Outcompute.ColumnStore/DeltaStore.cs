using System.Collections;

namespace Outcompute.ColumnStore;

// todo: configure serialization
// todo: this needs a factory
internal class DeltaStore<TRow> : IDeltaStore<TRow>
{
    private readonly IDeltaRowGroupFactory<TRow> _deltaRowGroupFactory;
    private readonly ColumnStoreOptions _options;

    public DeltaStore(ColumnStoreOptions options, IDeltaRowGroupFactory<TRow> deltaRowGroupFactory)
    {
        Guard.IsNotNull(options, nameof(options));
        Guard.IsNotNull(deltaRowGroupFactory, nameof(deltaRowGroupFactory));

        _options = options;
        _deltaRowGroupFactory = deltaRowGroupFactory;

        _active = _deltaRowGroupFactory.Create(_ids++, _options.RowGroupSizeThreshold);
        _groups.Add(_active);
    }

    private readonly List<IDeltaRowGroup<TRow>> _groups = new();

    private IDeltaRowGroup<TRow> _active;

    private bool _invalidated = true;

    private int _ids;

    public int Count { get; private set; }

    public void Add(TRow row)
    {
        if (_active.State == RowGroupState.Closed)
        {
            _active = _deltaRowGroupFactory.Create(_ids++, _options.RowGroupSizeThreshold);
            _groups.Add(_active);
        }

        _active.Add(row);

        _invalidated = true;

        Count++;
    }

    public int AddRange(IEnumerable<TRow> rows)
    {
        if (_active.State == RowGroupState.Closed)
        {
            _active = _deltaRowGroupFactory.Create(_ids++, _options.RowGroupSizeThreshold);
            _groups.Add(_active);
        }

        var before = _active.Count;

        _active.AddRange(rows);

        var added = _active.Count - before;

        Count += added;

        _invalidated = true;

        return added;
    }

    public void Close()
    {
        _active.Close();
    }

    public bool TryTakeClosed(out IRowGroup<TRow> group)
    {
        if (_groups.Count > 1)
        {
            foreach (var item in _groups)
            {
                if (item.State == RowGroupState.Closed)
                {
                    _groups.Remove(item);

                    _stats.RowCount -= item.Count;
                    _stats.RowGroupStats.Remove(item.Id);
                    _invalidated = true;

                    Count -= item.Count;

                    group = item;
                    return true;
                }
            }
        }

        group = default!;
        return false;
    }

    private readonly InnerStoreStats.Builder _stats = InnerStoreStats.CreateBuilder();

    public InnerStoreStats GetStats()
    {
        if (_invalidated)
        {
            _stats.RowCount = Count;

            foreach (var group in _groups)
            {
                _stats.RowGroupStats[group.Id] = group.Stats;
            }

            _invalidated = false;
        }

        return _stats.ToImmutable();
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