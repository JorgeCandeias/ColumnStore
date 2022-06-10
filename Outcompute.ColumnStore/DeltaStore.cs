using Orleans;
using System.Collections;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
internal class DeltaStore<TRow> : IDeltaStore<TRow>
{
    private readonly DeltaRowGroupFactory<TRow> _deltaRowGroupFactory;

    public DeltaStore(int rowGroupCapacity, DeltaRowGroupFactory<TRow> deltaRowGroupFactory)
    {
        Guard.IsGreaterThanOrEqualTo(rowGroupCapacity, 0, nameof(rowGroupCapacity));
        Guard.IsNotNull(deltaRowGroupFactory, nameof(deltaRowGroupFactory));

        _rowGroupCapacity = rowGroupCapacity;
        _deltaRowGroupFactory = deltaRowGroupFactory;
    }

    [Id(1)]
    private readonly int _rowGroupCapacity;

    [Id(2)]
    private int _ids;

    [Id(3)]
    private readonly List<DeltaRowGroup<TRow>> _groups = new();

    [Id(4)]
    public int Count { get; private set; }

    public void Add(TRow row)
    {
        GetOrAddRowGroup().Add(row);

        Invalidate();

        Count++;
    }

    public int AddRange(IEnumerable<TRow> rows)
    {
        var added = GetOrAddRowGroup().AddRange(rows);

        Count += added;

        Invalidate();

        return added;
    }

    public void Close()
    {
        if (_groups.Count > 0)
        {
            _groups[^1].Close();
        }
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

                    Count -= item.Count;
                    Invalidate();

                    group = item;
                    return true;
                }
            }
        }

        group = default!;
        return false;
    }

    private DeltaRowGroup<TRow> GetOrAddRowGroup()
    {
        if (_groups.Count > 0)
        {
            var group = _groups[^1];
            if (group.State is RowGroupState.Open)
            {
                return group;
            }
        }

        var created = _deltaRowGroupFactory.Create(_ids++, _rowGroupCapacity);
        _groups.Add(created);
        return created;
    }

    private InnerStoreStats? _stats;

    private InnerStoreStats BuildStats()
    {
        var builder = InnerStoreStats.CreateBuilder();

        builder.RowCount = Count;

        foreach (var group in _groups)
        {
            builder.RowGroupStats[group.Id] = group.Stats;
        }

        return builder.ToImmutable();
    }

    private void Invalidate()
    {
        _stats = null;
    }

    public InnerStoreStats Stats => _stats ??= BuildStats();

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