using System.Collections;

namespace Outcompute.ColumnStore;

/// <summary>
/// A generic column segment that supports any type.
/// </summary>
internal class ColumnSegment<TValue> : IColumnSegment<TValue>
{
    public ColumnSegment(string propertyName)
    {
        PropertyName = propertyName;
    }

    private readonly Dictionary<TValue, List<ColumnSegmentRange>> _ranges = new();

    private int _count;

    public string PropertyName { get; }

    public int Count => throw new NotImplementedException();

    public void Add(TValue value)
    {
        if (!_ranges.TryGetValue(value, out var ranges))
        {
            _ranges[value] = ranges = new List<ColumnSegmentRange>();
        }

        var last = ranges[^1];

        if (last.End == _count)
        {
            last.End += _count;
            ranges[^1] = last;
        }
    }

    public ColumnSegmentStats GetStats()
    {
        // todo
        throw new NotImplementedException();
    }

    public IEnumerator<TValue> GetEnumerator()
    {
        // todo
        throw new NotImplementedException();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}