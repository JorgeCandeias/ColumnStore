namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a sequence of values from a column.
/// </summary>
public interface IColumnSegment<out TValue> : IEnumerable<TValue>
{
    ColumnSegmentStats GetStats();
}