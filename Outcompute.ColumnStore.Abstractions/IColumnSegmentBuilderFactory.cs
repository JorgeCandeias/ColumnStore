namespace Outcompute.ColumnStore;

public interface IColumnSegmentBuilderFactory<TValue>
{
    IColumnSegmentBuilder<TValue> Create(IComparer<TValue> comparer);
}