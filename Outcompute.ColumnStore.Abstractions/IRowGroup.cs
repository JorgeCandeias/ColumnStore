namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a queryable group of rows.
/// </summary>
internal interface IRowGroup<out TRow> : IReadOnlyCollection<TRow>
{
    RowGroupState State { get; }
}