namespace Outcompute.ColumnStore;

internal interface IColumnSegmentBuilder<TValue>
{
    void Add(TValue value);

    IColumnSegment<TValue> ToImmutable();
}