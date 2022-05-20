using Microsoft.Extensions.Options;
using Orleans;
using System.Collections;
using System.Runtime.Serialization;

namespace Outcompute.ColumnStore;

/// <summary>
/// Holds an uncompressed group of rows.
/// </summary>
[GenerateSerializer]
[Obsolete("This class supports code generated and is not for application use")]
public abstract class DeltaRowGroup<TRow> : IRowGroup<TRow>
{
    private readonly ColumnStoreOptions _options;

    protected DeltaRowGroup(int id, IOptions<ColumnStoreOptions> options)
    {
        Guard.IsGreaterThanOrEqualTo(id, 0, nameof(id));
        Guard.IsNotNull(options, nameof(options));

        _options = options.Value;

        Id = id;
    }

    [Id(1)]
    public int Id { get; }

    [Id(2)]
    public RowGroupState State { get; private set; } = RowGroupState.Open;

    [Id(3)]
    protected IList<TRow> Rows = new List<TRow>();

    /// <summary>
    /// Gets distribution statistics about the stored data.
    /// </summary>
    public abstract IReadOnlyDictionary<string, DeltaColumnStats> GetStats();

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
    }

    public int Count => Rows.Count;

    public IEnumerator<TRow> GetEnumerator() => Rows.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}