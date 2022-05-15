namespace Outcompute.ColumnStore;

public class SegmentRange<TValue>
{
    public SegmentRange(TValue value)
    {
        Value = value;
    }

    public TValue Value { get; }

    public int Count { get; set; }
}