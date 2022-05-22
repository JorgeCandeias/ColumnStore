namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a sequence of values from a column.
/// </summary>
internal interface IColumnSegment<TValue> : IEnumerable<TValue>
{
    ColumnSegmentStats GetStats();
}