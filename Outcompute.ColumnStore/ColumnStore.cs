using System.Collections;

namespace Outcompute.ColumnStore;

public class ColumnStore<TRow> : IReadOnlyCollection<TRow> where TRow : new()
{
    private readonly RowGroupConverter<TRow> _converter = new();
    private readonly DeltaStore<TRow> _delta;
    private readonly List<CompressedRowGroup<TRow>> _groups = new();

    public ColumnStore()
    {
        _delta = new DeltaStore<TRow>(new ColumnStoreOptions());
    }

    public ColumnStore(ColumnStoreOptions options)
    {
        Guard.IsNotNull(options, nameof(options));

        _delta = new DeltaStore<TRow>(options);
    }

    public int Count { get; private set; }

    public void Add(TRow item)
    {
        _delta.Add(item);

        Count++;

        TryCompact();
    }

    public IEnumerator<TRow> GetEnumerator()
    {
        Console.WriteLine("Enumerating Compressed...");

        foreach (var group in _groups)
        {
            foreach (var row in group)
            {
                yield return row;
            }
        }

        Console.WriteLine("Enumerating Delta...");

        foreach (var row in _delta)
        {
            yield return row;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public ColumnStoreStats GetStats()
    {
        return new ColumnStoreStats(Count, _delta.Count, _groups.Select(x => x.GetStats()).ToList());
    }

    private void TryCompact()
    {
        // todo: the compression process is expensive when it happens and should run in the background
        if (_delta.TryTakeClosed(out var group))
        {
            _groups.Add(_converter.Convert(group));
        }
    }
}