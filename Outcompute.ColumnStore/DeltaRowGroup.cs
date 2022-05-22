using Microsoft.Extensions.Options;
using Orleans;
using System.Collections;
using System.Runtime.Serialization;

namespace Outcompute.ColumnStore;

/// <summary>
/// Holds an uncompressed group of rows.
/// </summary>
[GenerateSerializer]
public abstract class DeltaRowGroup<TRow> : IDeltaRowGroup<TRow>
{
    private readonly ColumnStoreOptions _options;

    protected DeltaRowGroup(int id, IOptions<ColumnStoreOptions> options)
    {
        Guard.IsGreaterThanOrEqualTo(id, 0, nameof(id));
        Guard.IsNotNull(options, nameof(options));

        _options = options.Value;

        Id = id;
        Stats.Id = id;
    }

    [Id(1)]
    public int Id { get; }

    [Id(2)]
    public RowGroupState State { get; private set; } = RowGroupState.Open;

    [Id(3)]
    protected IList<TRow> Rows = new List<TRow>();

    protected RowGroupStats.Builder Stats { get; } = RowGroupStats.CreateBuilder();

    /// <summary>
    /// Updated by generated classes to signal statistics invalidation.
    /// </summary>
    protected bool _invalidated;

    [OnDeserialized]
    private void OnDeserialized(StreamingContext context)
    {
        UpdateStats();
    }

    /// <summary>
    /// Updates row group and column statistics.
    /// </summary>
    private void UpdateStats()
    {
        Stats.RowCount = Rows.Count;

        OnUpdateStats();
    }

    /// <summary>
    /// Implemented by generated classes to update column statistics.
    /// </summary>
    protected abstract void OnUpdateStats();

    /// <summary>
    /// Gets distribution statistics about the stored data.
    /// </summary>
    public RowGroupStats GetStats()
    {
        if (_invalidated)
        {
            UpdateStats();

            _invalidated = false;
        }

        return Stats.ToImmutable();
    }

    /// <summary>
    /// Verifies that this row group is open and throws if it is not.
    /// </summary>
    private void EnsureNotOpen()
    {
        if (State != RowGroupState.Open)
        {
            ThrowHelper.ThrowInvalidOperationException($"This {nameof(DeltaRowGroup<TRow>)} is not open");
        }
    }

    /// <summary>
    /// Closes the row group if the row count has reached the threshold.
    /// </summary>
    private void TryClose()
    {
        if (Rows.Count >= _options.RowGroupSizeThreshold)
        {
            State = RowGroupState.Closed;
        }
    }

    /// <summary>
    /// Used by generated classes to update the live stats upon adding a row.
    /// </summary>
    protected abstract void OnAdded(TRow row);

    /// <summary>
    /// Adds the specified row to this row group.
    /// Will attempt to close the row group after consuming the row.
    /// </summary>
    public void Add(TRow row)
    {
        EnsureNotOpen();

        Rows.Add(row);

        OnAdded(row);

        TryClose();

        _invalidated = true;
    }

    /// <summary>
    /// Adds the specified batch to this row group.
    /// Will only attempt to close the row group after the entire batch is consumed.
    /// </summary>
    public void AddRange(IEnumerable<TRow> rows)
    {
        EnsureNotOpen();

        foreach (var row in rows)
        {
            Rows.Add(row);

            OnAdded(row);
        }

        TryClose();

        _invalidated = true;
    }

    public void Close()
    {
        State = RowGroupState.Closed;
    }

    public int Count => Rows.Count;

    public IEnumerator<TRow> GetEnumerator() => Rows.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}