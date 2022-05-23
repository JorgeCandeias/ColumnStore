namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a queryable group of rows.
/// </summary>
public interface IRowGroup<out TRow> : IReadOnlyCollection<TRow>
{
    int Id { get; }

    RowGroupState State { get; }

    IRowGroupStats Stats { get; }
}