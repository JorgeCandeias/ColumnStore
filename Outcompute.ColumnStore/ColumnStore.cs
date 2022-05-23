using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Collections;

namespace Outcompute.ColumnStore;

// todo: configure serialization
public class ColumnStore<TRow> : IColumnStore<TRow> where TRow : new()
{
    private readonly RowGroupConverter<TRow> _converter = new();
    private readonly IDeltaStore<TRow> _delta;
    private readonly ColumnStoreOptions _options;

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

    private ColumnStoreStats? _stats;

    private void ClearStats() => _stats = null;

    public IColumnStoreStats Stats => _stats ??= BuildStats();

    private ColumnStoreStats BuildStats()
    {
        var builder = ColumnStoreStats.CreateBuilder();

        builder.RowCount = Count;
        builder.DeltaStoreStats = _delta.GetStats();

        return builder.ToImmutable();
    }

    public void Add(TRow row)
    {
        _delta.Add(row);

        Count++;

        TryCompact();

        ClearStats();
    }

    public void AddRange(IEnumerable<TRow> rows)
    {
        Count += _delta.AddRange(rows);

        TryCompact();

        ClearStats();
    }

    public void Close()
    {
        _delta.Close();

        // todo: migrate

        ClearStats();
    }

    public void Rebuild()
    {
        // todo: rebuild row groups
        throw new NotImplementedException();

        ClearStats();
    }

    public IEnumerator<TRow> GetEnumerator()
    {
        Console.WriteLine("Enumerating Solid...");

        // todo

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
            // todo
        }
    }
}