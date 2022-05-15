namespace Outcompute.ColumnStore;

/// <summary>
/// A generic column segment that supports any type.
/// </summary>
internal class ColumnSegment<TValue>
    where TValue : IEquatable<TValue>
{
    public ColumnSegment(string propertyName)
    {
        PropertyName = propertyName;
    }

    private readonly List<SegmentRange<TValue>> _ranges = new();

    private SegmentRange<TValue>? _last;

    private int _count;

    public string PropertyName { get; }

    public void Add(TValue value)
    {
        // handle the first item or a different item
        if (_ranges.Count == 0 || !_last!.Value.Equals(value))
        {
            _last = new SegmentRange<TValue>(value)
            {
                Count = 1
            };

            _ranges.Add(_last);
        }
        else
        {
            // handle repeat item
            _last.Count++;
        }

        _count++;
    }

    public IEnumerable<TValue> EnumerateRows()
    {
        for (var i = 0; i < _ranges.Count; i++)
        {
            var range = _ranges[i];

            for (var j = 0; j < range.Count; j++)
            {
                yield return range.Value;
            }
        }
    }

    public IEnumerable<SegmentRange<TValue>> EnumerateRanges()
    {
        return _ranges;
    }

    public ColumnSegmentStats GetStats()
    {
        return new ColumnSegmentStats(PropertyName, _count, _ranges.Count);
    }
}