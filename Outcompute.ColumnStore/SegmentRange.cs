namespace Outcompute.ColumnStore;

internal class SegmentRange<TValue>
{
    public SegmentRange(TValue value)
    {
        Value = value;
    }

    public TValue Value { get; }

    public int Count { get; set; }
}