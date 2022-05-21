namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a dataset stored in a column oriented format.
/// </summary>
public interface IColumnStore<TRow> : IReadOnlyCollection<TRow>
{
    /// <summary>
    /// Adds a new row to a delta row group in this column store.
    /// If the total rows in the delta row group meet the compression threshold then the row group is also compressed.
    /// </summary>
    void Add(TRow row);

    /// <summary>
    /// Adds the specified rows to a delta row group in this column store.
    /// If the total rows in the delta row group meet the compression threshold then the row group is also compressed.
    /// </summary>
    void AddRange(IEnumerable<TRow> rows);

    /// <summary>
    /// Closes the active delta row group and forces compression.
    /// </summary>
    void Close();

    /// <summary>
    /// Rebuilds the entire data set into a single compressed row group.
    /// </summary>
    void Rebuild();
}