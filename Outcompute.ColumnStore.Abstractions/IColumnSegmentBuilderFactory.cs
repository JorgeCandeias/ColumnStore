namespace Outcompute.ColumnStore;

internal interface IColumnSegmentBuilderFactory<TValue>
{
    IColumnSegmentBuilder<TValue> Create(IComparer<TValue> comparer);
}