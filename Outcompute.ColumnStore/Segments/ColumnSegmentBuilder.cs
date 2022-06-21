using Microsoft.IO;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Outcompute.ColumnStore.Segments;

internal abstract class ColumnSegmentBuilder<T>
{
    private readonly Dictionary<KeyWrapper, List<Range>> _groups;
    private readonly SerializerSessionPool _sessions;

    protected ColumnSegmentBuilder(IComparer<T> comparer, IServiceProvider provider, SerializerSessionPool sessions)
    {
        Guard.IsNotNull(comparer, nameof(comparer));
        Guard.IsNotNull(provider, nameof(provider));
        Guard.IsNotNull(sessions, nameof(sessions));

        Comparer = comparer;
        _sessions = sessions;

        ServiceProvider = provider;

        _groups = new(new KeyWrapperComparer(comparer));
    }

    protected IServiceProvider ServiceProvider { get; }

    protected IComparer<T> Comparer { get; }

    private readonly ColumnSegmentStats.Builder _stats = ColumnSegmentStats.CreateBuilder();

    public int Count => _stats.RowCount;

    /// <summary>
    /// Defers key comparisons to the underlying key type being wrapped to support null dictionary keys.
    /// </summary>
    private sealed class KeyWrapperComparer : IComparer<KeyWrapper>, IEqualityComparer<KeyWrapper>
    {
        private readonly IComparer<T> _comparer;

        public KeyWrapperComparer(IComparer<T> comparer)
        {
            _comparer = comparer;
        }

        public int Compare(ColumnSegmentBuilder<T>.KeyWrapper x, ColumnSegmentBuilder<T>.KeyWrapper y)
        {
            return _comparer.Compare(x.Value, y.Value);
        }

        public bool Equals(ColumnSegmentBuilder<T>.KeyWrapper x, ColumnSegmentBuilder<T>.KeyWrapper y)
        {
            return _comparer.Compare(x.Value, y.Value) == 0;
        }

        public int GetHashCode([DisallowNull] ColumnSegmentBuilder<T>.KeyWrapper obj)
        {
            return HashCode.Combine(obj.Value);
        }
    }

    /// <summary>
    /// Wraps the column value type to allow null dictionary keys.
    /// </summary>
    private readonly record struct KeyWrapper(T Value);

    private sealed class Range
    {
        public int Start { get; set; }
        public int End { get; set; }
    }

    private sealed class RangeComparer : IComparer<Range>
    {
        public int Compare(ColumnSegmentBuilder<T>.Range? x, ColumnSegmentBuilder<T>.Range? y)
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

        _stats.RangeCount++;
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
            Initialize(list);
        }
    }

    public void Add(T value)
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

        if (Comparer.Compare(key.Value, default) == 0)
        {
            _stats.DefaultValueCount++;
        }

        _stats.RowCount++;
    }

    public string Name { get; set; } = Empty;

    public ColumnSegment<T> ToImmutable()
    {
        var stream = ColumnStoreMemoryStreamManager.GetStream();
        using var session = _sessions.GetSession();
        var writer = Writer.Create<IBufferWriter<byte>>(stream, session);

        // todo: this payload is not yet version tolerant
        // todo: trial adding version tolerance while comparing payload size

        // prefix with the total group count
        writer.WriteVarUInt32((uint)_groups.Count);

        // prefix with the total range count
        writer.WriteVarUInt32((uint)_groups.Sum(x => x.Value.Count));

        // write each group
        foreach (var item in _groups)
        {
            // write the group value
            OnSerializeValue(item.Key.Value, ref writer);

            // prefix with the range count
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

        return OnCreate(stream, _stats.ToImmutable());
    }

    protected abstract void OnSerializeValue(T value, ref Writer<IBufferWriter<byte>> writer);

    protected abstract ColumnSegment<T> OnCreate(RecyclableMemoryStream stream, ColumnSegmentStats stats);
}