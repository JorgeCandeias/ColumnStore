using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections;

namespace Outcompute.ColumnStore;

// todo: configure serialization
public class ColumnStore<TRow> : IColumnStore<TRow> where TRow : new()
{
    private readonly RowGroupConverter<TRow> _converter = new();
    private readonly IDeltaStore<TRow> _delta;
    private readonly List<CompressedRowGroup<TRow>> _groups = new();
    private readonly ColumnStoreOptions _options;
    private readonly ColumnStoreStats.Builder _stats = ColumnStoreStats.CreateBuilder();

    public ColumnStore(int rowGroupSizeThreshold = 1_000_000)
    {
        _options = new ColumnStoreOptions
        {
            RowGroupSizeThreshold = rowGroupSizeThreshold
        };

        _delta = ActivatorUtilities.CreateInstance<DeltaStore<TRow>>(FallbackServiceProvider.Default);
    }

    internal ColumnStore(IOptions<ColumnStoreOptions> options, IDeltaStore<TRow> delta)
    {
        _options = options.Value;
        _delta = delta;
    }

    public int Count { get; private set; }

    private bool _invalidated;

    private void UpdateStats()
    {
        _stats.RowCount = Count;
        _stats.DeltaStoreStats = _delta.GetStats();
    }

    public void Add(TRow row)
    {
        _delta.Add(row);

        Count++;

        TryCompact();

        _invalidated = true;
    }

    public void AddRange(IEnumerable<TRow> rows)
    {
        Count += _delta.AddRange(rows);

        TryCompact();

        _invalidated = true;
    }

    public void Close()
    {
        _delta.Close();

        // todo: compress row groups
        throw new NotImplementedException();

        _invalidated = true;
    }

    public void Rebuild()
    {
        // todo: rebuild row groups
        throw new NotImplementedException();

        _invalidated = true;
    }

    public ColumnStoreStats GetStats()
    {
        if (_invalidated)
        {
            UpdateStats();

            _invalidated = false;
        }

        return _stats.ToImmutable();
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

    private void TryCompact()
    {
        // todo: the compression process is expensive when it happens and should run in the background
        if (_delta.TryTakeClosed(out var group))
        {
            _groups.Add(_converter.Convert(group));
        }
    }
}