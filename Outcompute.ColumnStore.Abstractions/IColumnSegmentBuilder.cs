namespace Outcompute.ColumnStore;

public interface IColumnSegmentBuilder<TValue>
{
    void Add(TValue value);

    IColumnSegment<TValue> ToImmutable();
}