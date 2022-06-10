using Microsoft.Extensions.Options;
using System.Collections;

namespace Outcompute.ColumnStore;

// todo: configure serialization
internal class ColumnStore<TRow> : IColumnStore<TRow> where TRow : new()
{
    private readonly RowGroupConverter<TRow> _converter = new();
    private readonly IDeltaStore<TRow> _delta;
    private readonly ColumnStoreOptions _options;

    /// <summary>
    /// Dependency constructor for service-like usage.
    /// </summary>
    internal ColumnStore(IOptions<ColumnStoreOptions> options, IDeltaStoreFactory<TRow> deltaStoreFactory)
    {
        _options = options.Value;
        _delta = deltaStoreFactory.Create(options.Value.RowGroupSizeThreshold);
    }

    public int Count { get; private set; }

    private ColumnStoreStats? _stats;

    private void ClearStats() => _stats = null;

    public IColumnStoreStats Stats => _stats ??= BuildStats();

    private ColumnStoreStats BuildStats()
    {
        var builder = ColumnStoreStats.CreateBuilder();

        builder.RowCount = Count;
        builder.DeltaStoreStats = _delta.Stats;

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