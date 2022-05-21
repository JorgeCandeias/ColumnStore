namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a queryable group of rows.
/// </summary>
internal interface IRowGroup<TRow> : IReadOnlyCollection<TRow>
{
    int Id { get; }

    RowGroupState State { get; }

    void Add(TRow row);

    void AddRange(IEnumerable<TRow> rows);

    void Close();

    RowGroupStats GetStats();
}