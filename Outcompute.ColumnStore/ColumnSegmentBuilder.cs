using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;

namespace Outcompute.ColumnStore;

internal class ColumnSegmentBuilder<TValue>
{
    private readonly IComparer<TValue> _comparer;
    private readonly Dictionary<KeyWrapper, List<Range>> _groups;
    private readonly SerializerSessionPool _sessions;
    private readonly Serializer<ColumnSegmentHeader<TValue>> _headerSerializer;
    private readonly Serializer<ColumnSegmentRange> _rangeSerializer;

    public ColumnSegmentBuilder(IComparer<TValue> comparer, SerializerSessionPool sessions, Serializer<ColumnSegmentHeader<TValue>> headerSerializer, Serializer<ColumnSegmentRange> rangeSerializer)
    {
        Guard.IsNotNull(comparer, nameof(comparer));
        Guard.IsNotNull(sessions, nameof(sessions));
        Guard.IsNotNull(headerSerializer, nameof(headerSerializer));
        Guard.IsNotNull(rangeSerializer, nameof(rangeSerializer));

        _comparer = comparer;
        _sessions = sessions;
        _headerSerializer = headerSerializer;
        _rangeSerializer = rangeSerializer;

        _groups = new(new KeyWrapperEqualityComparer(comparer));
    }

    private readonly ColumnSegmentStats.Builder _stats = ColumnSegmentStats.CreateBuilder();
    private int _count;

    /// <summary>
    /// Defers key comparisons to the underlying key type being wrapped to support null dictionary keys.
    /// </summary>
    private sealed class KeyWrapperEqualityComparer : IEqualityComparer<KeyWrapper>
    {
        private readonly IComparer<TValue> _comparer;

        public KeyWrapperEqualityComparer(IComparer<TValue> comparer)
        {
            _comparer = comparer;
        }

        public bool Equals(KeyWrapper x, KeyWrapper y) => _comparer.Compare(x.Value, y.Value) == 0;

        public int GetHashCode(KeyWrapper obj) => HashCode.Combine(obj.Value);
    }

    /// <summary>
    /// Wraps the column value type to allow null dictionary keys.
    /// </summary>
    private record struct KeyWrapper(TValue Value);

    private sealed class Range
    {
        public int Start { get; set; }
        public int End { get; set; }
    }

    private void Initialize(List<Range> list)
    {
        list.Add(new Range
        {
            Start = _count,
            End = _count
        });

        _count++;
    }

    private void Increment(List<Range> list)
    {
        var last = list[^1];

        if (last.End == _count - 1)
        {
            last.End = _count;
        }
        else
        {
            list.Add(new Range
            {
                Start = _count,
                End = _count
            });
        }

        _count++;
    }

    public void Add(TValue value)
    {
        var key = new KeyWrapper(value);

        if (_groups.TryGetValue(key, out var list))
        {
            Increment(list);
        }
        else
        {
            _groups[key] = list = new();

            Initialize(list);
        }

        if (_comparer.Compare(key.Value, default) == 0)
        {
            _stats.DefaultValueCount++;
        }

        _count++;
    }

    public string Name { get; set; } = Empty;

    public ColumnSegment<TValue> ToImmutable()
    {
        var stream = ColumnStoreMemoryStreamManager.GetStream();
        using var session = _sessions.GetSession();
        var writer = Writer.Create(stream, session);

        foreach (var item in _groups)
        {
            _headerSerializer.Serialize(new ColumnSegmentHeader<TValue>(item.Key.Value, item.Value.Count), ref writer);

            foreach (var range in item.Value)
            {
                _rangeSerializer.Serialize(new ColumnSegmentRange(range.Start, range.End), ref writer);
            }
        }

        _stats.Name = Name;
        _stats.DistinctValueCount = _groups.Count;

        return new ColumnSegment<TValue>(stream, _stats.ToImmutable(), _headerSerializer, _rangeSerializer, _sessions);
    }
}