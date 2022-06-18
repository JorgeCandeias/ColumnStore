using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Diagnostics.CodeAnalysis;

namespace Outcompute.ColumnStore;

internal class ColumnSegmentBuilder<TValue>
{
    private readonly IComparer<TValue> _comparer;
    private readonly Dictionary<KeyWrapper, List<Range>> _groups;
    private readonly SerializerSessionPool _sessions;
    private readonly Serializer<ColumnSegmentHeader<TValue>> _headerSerializer;
    private readonly Serializer<ColumnSegmentRange> _rangeSerializer;
    private readonly Serializer<TValue> _valueSerializer;

    public ColumnSegmentBuilder(IComparer<TValue> comparer, SerializerSessionPool sessions, Serializer<ColumnSegmentHeader<TValue>> headerSerializer, Serializer<ColumnSegmentRange> rangeSerializer, Serializer<TValue> valueSerializer)
    {
        Guard.IsNotNull(comparer, nameof(comparer));
        Guard.IsNotNull(sessions, nameof(sessions));
        Guard.IsNotNull(headerSerializer, nameof(headerSerializer));
        Guard.IsNotNull(rangeSerializer, nameof(rangeSerializer));
        Guard.IsNotNull(valueSerializer, nameof(valueSerializer));

        _comparer = comparer;
        _sessions = sessions;
        _headerSerializer = headerSerializer;
        _rangeSerializer = rangeSerializer;
        _valueSerializer = valueSerializer;

        _groups = new(new KeyWrapperComparer(comparer));
    }

    private readonly ColumnSegmentStats.Builder _stats = ColumnSegmentStats.CreateBuilder();

    public int Count { get; private set; }

    /// <summary>
    /// Defers key comparisons to the underlying key type being wrapped to support null dictionary keys.
    /// </summary>
    private sealed class KeyWrapperComparer : IComparer<KeyWrapper>, IEqualityComparer<KeyWrapper>
    {
        private readonly IComparer<TValue> _comparer;

        public KeyWrapperComparer(IComparer<TValue> comparer)
        {
            _comparer = comparer;
        }

        public int Compare(ColumnSegmentBuilder<TValue>.KeyWrapper x, ColumnSegmentBuilder<TValue>.KeyWrapper y)
        {
            return _comparer.Compare(x.Value, y.Value);
        }

        public bool Equals(ColumnSegmentBuilder<TValue>.KeyWrapper x, ColumnSegmentBuilder<TValue>.KeyWrapper y)
        {
            return _comparer.Compare(x.Value, y.Value) == 0;
        }

        public int GetHashCode([DisallowNull] ColumnSegmentBuilder<TValue>.KeyWrapper obj)
        {
            return HashCode.Combine(obj.Value);
        }
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

    private sealed class RangeComparer : IComparer<Range>
    {
        public int Compare(ColumnSegmentBuilder<TValue>.Range? x, ColumnSegmentBuilder<TValue>.Range? y)
        {
            if (x is null)
            {
                return y is null ? 0 : -1;
            }
            else if (y is null)
            {
                return 1;
            }
            else if (x.Start < y.Start)
            {
                return -1;
            }
            else if (x.Start > y.Start)
            {
                return 1;
            }
            else if (x.End < y.End)
            {
                return -1;
            }
            else if (x.End > y.End)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        public static RangeComparer Default { get; } = new();
    }

    private void Initialize(List<Range> list)
    {
        list.Add(new Range
        {
            Start = Count,
            End = Count
        });
    }

    private void Increment(List<Range> list)
    {
        var last = list[^1];

        if (last.End == Count - 1)
        {
            last.End = Count;
        }
        else
        {
            list.Add(new Range
            {
                Start = Count,
                End = Count
            });
        }
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

        Count++;
    }

    public string Name { get; set; } = Empty;

    public ColumnSegment<TValue> ToImmutable()
    {
        var stream = ColumnStoreMemoryStreamManager.GetStream();
        using var session = _sessions.GetSession();
        var writer = Writer.Create(stream, session);

        // write the total group count
        writer.WriteVarUInt32((uint)_groups.Count);

        // write the total range count
        writer.WriteVarUInt32((uint)_groups.Sum(x => x.Value.Count));

        // write each group
        foreach (var item in _groups)
        {
            // write the group value
            _valueSerializer.Serialize(item.Key.Value, ref writer);

            // write the range count
            writer.WriteVarUInt32((uint)item.Value.Count);

            // write each range
            foreach (var range in item.Value)
            {
                // write the start
                writer.WriteVarUInt32((uint)range.Start);

                // write the end
                writer.WriteVarUInt32((uint)range.End);
            }
        }

        _stats.Name = Name;
        _stats.DistinctValueCount = _groups.Count;

        return new ColumnSegment<TValue>(stream, _stats.ToImmutable(), _headerSerializer, _rangeSerializer, _valueSerializer, _sessions);
    }
}