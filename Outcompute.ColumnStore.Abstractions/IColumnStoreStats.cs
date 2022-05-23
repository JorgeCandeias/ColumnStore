namespace Outcompute.ColumnStore;

public interface IColumnStoreStats
{
    int RowCount { get; }

    IInnerStoreStats DeltaStoreStats { get; }

    IInnerStoreStats SolidStoreStats { get; }
}