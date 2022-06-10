namespace Outcompute.ColumnStore;

public interface IRowGroupStats
{
    public int Id { get; }

    public int RowCount { get; }

    public IReadOnlyDictionary<string, IColumnSegmentStats> ColumnSegmentStats { get; }
}