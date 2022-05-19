namespace Outcompute.ColumnStore;

/// <summary>
/// A generic column segment that supports any type.
/// </summary>
internal class ColumnSegment<TValue> where TValue : IComparable<TValue>
{
    public ColumnSegment(string propertyName)
    {
        PropertyName = propertyName;
    }

    private readonly List<ColumnSegmentRange<TValue>> _ranges = new();

    private ColumnSegmentRange<TValue>? _last;

    private int _count;

    public string PropertyName { get; }

    public void Add(TValue value)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<TValue> EnumerateRows()
    {
        for (var i = 0; i < _ranges.Count; i++)
        {
            var range = _ranges[i];

            for (var j = range.Start; j < range.End; j++)
            {
                yield return range.Value;
            }
        }
    }

    public IEnumerable<ColumnSegmentRange<TValue>> EnumerateRanges()
    {
        return _ranges;
    }

    public ColumnSegmentStats GetStats()
    {
        //return new ColumnSegmentStats(PropertyName, _count, _ranges.Count);
        throw new NotImplementedException();
    }
}