using CommunityToolkit.HighPerformance.Buffers;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Buffers;

namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// An encoding that builds a dictionary of unique values and maps them to their ranges.
/// This encoding can produce lower payload size than plain encoding for large values with low value cardinality.
/// </summary>
internal class DictionaryEncoding<T> : Encoding<T>
{
    private readonly Serializer<T> _serializer;
    private readonly SerializerSessionPool _sessions;

    public DictionaryEncoding(Serializer<T> serializer, SerializerSessionPool sessions)
    {
        Guard.IsNotNull(serializer, nameof(serializer));
        Guard.IsNotNull(sessions, nameof(sessions));

        _serializer = serializer;
        _sessions = sessions;
    }

    public override void Encode(IBufferWriter<byte> writer, ReadOnlySequence<T> sequence)
    {
        Guard.IsNotNull(writer, nameof(writer));
        Guard.IsLessThanOrEqualTo(sequence.Length, int.MaxValue, nameof(sequence.Length));

        // create writing artefacts
        using var session = _sessions.GetSession();
        var xwriter = Writer.Create(writer, session);

        // prefix with row count
        xwriter.WriteVarUInt32((uint)sequence.Length);

        // break early if there no values to write
        if (sequence.Length == 0)
        {
            return;
        }

        // build the value dictionary
        var lookup = new SortedDictionary<KeyWrapper, List<DictionaryRange>>();

        // read the first item
        T current = default!;
        var s = 0;
        var l = 1;
        foreach (var memory in sequence.Slice(0, 1))
        {
            foreach (var value in memory.Span)
            {
                current = value;
            }
        }

        // now run range detection on the rest
        var comparer = EqualityComparer<T>.Default;
        foreach (var memory in sequence.Slice(1))
        {
            foreach (var value in memory.Span)
            {
                // see if we changed range
                if (comparer.Equals(value, current))
                {
                    l++;
                }
                else
                {
                    // close the previous range
                    AddRange(lookup, current, s, l);

                    // start a new range
                    current = value;
                    s += l;
                    l = 1;
                }
            }
        }

        // close the last range (or first if there were no others)
        AddRange(lookup, current, s, l);

        // write the keys in order
        xwriter.WriteVarUInt32((uint)lookup.Count);
        foreach (var item in lookup.Keys)
        {
            _serializer.Serialize(item.Value, ref xwriter);
        }

        // write the ranges in order
        foreach (var item in lookup)
        {
            // write a range block
            xwriter.WriteUInt32((uint)lookup.Values.Count);
            foreach (var range in item.Value)
            {
                xwriter.WriteVarUInt32((uint)range.Start);
                xwriter.WriteVarUInt32((uint)range.Length);
            }
        }
    }

    public override MemoryOwner<T> Decode(ReadOnlySequence<byte> sequence)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(sequence, session);

        // read row count
        var rowCount = (int)reader.ReadVarUInt32();

        // break early if there is nothing to read
        if (rowCount is 0)
        {
            return MemoryOwner<T>.Empty;
        }

        // read total keys
        var keyCount = (int)reader.ReadVarUInt32();

        // read all keys
        using var keyBuffer = MemoryOwner<T>.Allocate(keyCount);
        for (var i = 0; i < keyCount; i++)
        {
            keyBuffer.Span[i] = _serializer.Deserialize(ref reader);
        }

        // read and order all ranges
        var sorted = new SortedSet<SortableRange>();
        for (int k = 0; k < keyCount; k++)
        {
            var rangeCount = (int)reader.ReadVarUInt32();
            for (var r = 0; r < rangeCount; r++)
            {
                var start = (int)reader.ReadVarUInt32();
                var length = (int)reader.ReadVarUInt32();
                sorted.Add(new SortableRange(start, length, k));
            }
        }

        // yield the result
        var result = MemoryOwner<T>.Allocate(rowCount);
        var span = result.Span;
        var v = 0;
        foreach (var item in sorted)
        {
            for (var i = item.Start; i < item.Length; i++)
            {
                span[v++] = keyBuffer.Span[item.Index];
            }
        }
        return result;
    }

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySequence<byte> sequence, T value)
    {
        throw new NotImplementedException();
    }

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySequence<byte> sequence, int start, int length)
    {
        throw new NotImplementedException();
    }

    private record struct DictionaryRange(int Start, int Length);

    private record struct SortableRange(int Start, int Length, int Index) : IComparable<SortableRange>
    {
        public int CompareTo(DictionaryEncoding<T>.SortableRange other) => Start.CompareTo(other.Start);
    }

    private record struct KeyWrapper(T Value);

    private static void AddRange(SortedDictionary<KeyWrapper, List<DictionaryRange>> lookup, T value, int start, int length)
    {
        var key = new KeyWrapper(value);
        if (!lookup.TryGetValue(key, out var ranges))
        {
            lookup[key] = ranges = new List<DictionaryRange>();
        }

        // close the previous range
        ranges.Add(new DictionaryRange(start, length));
    }
}