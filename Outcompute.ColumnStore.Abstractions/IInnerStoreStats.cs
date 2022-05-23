namespace Outcompute.ColumnStore;

public interface IInnerStoreStats
{
    int RowCount { get; }

    IReadOnlyDictionary<int, IRowGroupStats> RowGroupStats { get; }
}