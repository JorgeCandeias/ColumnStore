namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a queryable group of rows.
/// </summary>
public interface IRowGroup<TRow> : IReadOnlyCollection<TRow>
{
    int Id { get; }

    RowGroupState State { get; }

    RowGroupStats GetStats();
}