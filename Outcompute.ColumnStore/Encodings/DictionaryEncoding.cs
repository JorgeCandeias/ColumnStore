using CommunityToolkit.HighPerformance.Buffers;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Buffers;

namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// An encoding that builds a dictionary of unique values and maps them to their ranges.
/// Ranges are compressed via run length encoding.
/// This encoding can produce a smaller payload size than plain encoding for sequences with low value cardinality.
/// This encoding will deduplicate values that look different but compare the same.
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

    public override void Encode(ReadOnlySequence<T> sequence, IBufferWriter<byte> writer)
    {
        Guard.IsNotNull(writer, nameof(writer));
        Guard.IsLessThanOrEqualTo(sequence.Length, int.MaxValue, nameof(sequence.Length));

        // create writing artefacts
        using var session = _sessions.GetSession();
        var xwriter = Writer.Create(writer, session);

        // prefix with encoding id
        xwriter.WriteVarUInt32((uint)WellKnownEncodings.Dictionary);

        // prefix with row count
        xwriter.WriteVarUInt32((uint)sequence.Length);

        // break early if there are no values to write
        if (sequence.Length == 0)
        {
            return;
        }

        // create the running structures
        var lookup = new Dictionary<KeyWrapper, int>();
        using var ranges = MemoryOwner<DictionaryRange>.Allocate((int)sequence.Length);
        var span = ranges.Span;
        var added = 0;

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
                    AddRange(lookup, span, ref added, current, l);

                    // start a new range
                    current = value;
                    s += l;
                    l = 1;
                }
            }
        }

        // close the last range (or first if there were no others)
        AddRange(lookup, span, ref added, current, l);

        // write the keys in index order
        xwriter.WriteVarUInt32((uint)lookup.Count);
        foreach (var item in lookup.OrderBy(x => x.Value))
        {
            _serializer.Serialize(item.Key.Value, ref xwriter);
        }

        // write the ranges in source order
        xwriter.WriteUInt32((uint)lookup.Values.Count);
        foreach (var item in span)
        {
            xwriter.WriteVarUInt32((uint)item.Index);
            xwriter.WriteVarUInt32((uint)item.Length);
        }

        static void AddRange(Dictionary<KeyWrapper, int> lookup, Span<DictionaryRange> span, ref int added, T value, int length)
        {
            var key = new KeyWrapper(value);
            if (!lookup.TryGetValue(key, out var index))
            {
                lookup[key] = index = lookup.Count;
            }
            span[added++] = new DictionaryRange(index, length);
        }
    }

    public override MemoryOwner<T> Decode(ReadOnlySequence<byte> sequence)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(sequence, session);

        VerifyEncodingId(ref reader);

        // read row count
        var rowCount = (int)reader.ReadVarUInt32();

        // break early if there is nothing to read
        if (rowCount is 0)
        {
            return MemoryOwner<T>.Empty;
        }

        // read keys
        var keyCount = (int)reader.ReadVarUInt32();
        var keyBuffer = ArrayPool<T>.Shared.Rent(keyCount);
        for (var i = 0; i < keyCount; i++)
        {
            keyBuffer[i] = _serializer.Deserialize(ref reader);
        }

        // read ranges
        var rangeCount = (int)reader.ReadVarUInt32();
        var result = MemoryOwner<T>.Allocate(rowCount);
        var span = result.Span;
        var added = 0;
        for (var r = 0; r < rangeCount; r++)
        {
            var index = (int)reader.ReadVarUInt32();
            var length = (int)reader.ReadVarUInt32();
            var value = keyBuffer[index];
            for (var i = 0; i < length; i++)
            {
                span[added++] = value;
            }
        }

        // cleanup
        ArrayPool<T>.Shared.Return(keyBuffer, true);

        return result;
    }

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySequence<byte> sequence, T value)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(sequence, session);

        VerifyEncodingId(ref reader);

        // read row count
        var rowCount = (int)reader.ReadVarUInt32();

        // break early if there is nothing to read
        if (rowCount is 0)
        {
            return MemoryOwner<ValueRange<T>>.Empty;
        }

        // read keys and create filter lookup
        var keyCount = (int)reader.ReadVarUInt32();
        var keyBuffer = ArrayPool<T>.Shared.Rent(keyCount);
        var filterBuffer = ArrayPool<bool>.Shared.Rent(keyCount);
        var keyComparer = EqualityComparer<T>.Default;
        for (var i = 0; i < keyCount; i++)
        {
            var key = _serializer.Deserialize(ref reader);
            keyBuffer[i] = key;
            filterBuffer[i] = keyComparer.Equals(key, value);
        }

        // read ranges and filter by index lookup
        var rangeCount = (int)reader.ReadVarUInt32();
        var result = MemoryOwner<ValueRange<T>>.Allocate(rowCount);
        var span = result.Span;
        var added = 0;
        var start = 0;
        for (var r = 0; r < rangeCount; r++)
        {
            var index = (int)reader.ReadVarUInt32();
            var length = (int)reader.ReadVarUInt32();

            if (filterBuffer[index])
            {
                span[added++] = new ValueRange<T>(keyBuffer[index], start, length);
            }

            start += length;
        }

        // cleanup
        ArrayPool<T>.Shared.Return(keyBuffer, true);
        ArrayPool<bool>.Shared.Return(filterBuffer, false);

        return result[..added];
    }

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySequence<byte> sequence, int start, int length)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(sequence, session);

        VerifyEncodingId(ref reader);

        // break early if there is nothing to read
        int rowCount = ReadRowCount(ref reader);
        if (rowCount is 0)
        {
            return MemoryOwner<ValueRange<T>>.Empty;
        }

        // read keys and create filter lookup
        var keyCount = (int)reader.ReadVarUInt32();
        var keyBuffer = ArrayPool<T>.Shared.Rent(keyCount);
        for (var i = 0; i < keyCount; i++)
        {
            keyBuffer[i] = _serializer.Deserialize(ref reader);
        }

        // read ranges and filter by index lookup
        var rangeCount = (int)reader.ReadVarUInt32();
        var result = MemoryOwner<ValueRange<T>>.Allocate(rowCount);
        var span = result.Span;
        var added = 0;
        var readStart = 0;
        var targetEnd = start + length;
        for (var r = 0; r < rangeCount; r++)
        {
            // break early if we went past the target range
            if (readStart >= targetEnd)
            {
                break;
            }

            var readIndex = (int)reader.ReadVarUInt32();
            var readLength = (int)reader.ReadVarUInt32();
            var candidateStart = Math.Max(readStart, start);
            var candidateEnd = Math.Min(readStart + readLength, targetEnd);

            if (candidateStart < candidateEnd)
            {
                span[added++] = new ValueRange<T>(keyBuffer[readIndex], candidateStart, candidateEnd - candidateStart);
            }

            readStart += readLength;
        }

        // cleanup
        ArrayPool<T>.Shared.Return(keyBuffer, true);

        return result[..added];
    }

    private record struct DictionaryRange(int Index, int Length);

    private record struct SortableRange(int Start, int Length, int Index) : IComparable<SortableRange>
    {
        public int CompareTo(DictionaryEncoding<T>.SortableRange other) => Start.CompareTo(other.Start);
    }

    private record struct KeyWrapper(T Value);

    private static void VerifyEncodingId(ref Reader<ReadOnlySequence<byte>> reader)
    {
        var id = reader.ReadVarUInt32();
        if (id != (int)WellKnownEncodings.Dictionary)
        {
            ThrowHelper.ThrowInvalidOperationException($"Payload does not start with the encoding marker of '{(int)WellKnownEncodings.Dictionary}'");
        }
    }

    private static int ReadRowCount(ref Reader<ReadOnlySequence<byte>> reader) => (int)reader.ReadVarUInt32();
}