using Orleans.Serialization.Buffers;
using System.IO.Pipelines;

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

    public override void Encode<TBufferWriter>(ReadOnlySpan<T> source, TBufferWriter bufferWriter)
    {
        // create writing artefacts
        using var session = _sessions.GetSession();
        var writer = Writer.Create(bufferWriter, session);

        // prefix with encoding id
        writer.WriteVarUInt32((uint)WellKnownEncodings.Dictionary);

        // prefix with row count
        writer.WriteVarUInt32((uint)source.Length);

        // write rows
        if (source.Length > 0)
        {
            // create the running structures
            var lookup = new Dictionary<KeyWrapper, int>();
            using var ranges = SpanOwner<DictionaryRange>.Allocate(source.Length);
            var span = ranges.Span;
            var added = 0;

            // read the first item
            var current = source[0];
            var s = 0;
            var l = 1;

            // now run range detection on the rest
            var comparer = EqualityComparer<T>.Default;
            foreach (var value in source[1..])
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

            // close the last range (or first if there were no others)
            AddRange(lookup, span, ref added, current, l);

            // write the keys in index order
            writer.WriteVarUInt32((uint)lookup.Count);
            foreach (var item in lookup.OrderBy(x => x.Value))
            {
                _serializer.Serialize(item.Key.Value, ref writer);
            }

            // write the ranges in source order
            writer.WriteVarUInt32((uint)added);
            for (var i = 0; i < added; i++)
            {
                var item = span[i];

                writer.WriteVarUInt32((uint)item.Index);
                writer.WriteVarUInt32((uint)item.Length);
            }
        }

        writer.Commit();

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

    public override IMemoryOwner<T> Decode(ReadOnlySpan<byte> source)
    {
        // todo
        /*
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

        VerifyEncodingId(ref reader);

        // read row count
        var rowCount = (int)reader.ReadVarUInt32();

        // break early if there is nothing to read
        if (rowCount is 0)
        {
            return SpanOwner<T>.Empty;
        }

        // read keys
        var keyCount = (int)reader.ReadVarUInt32();
        using var keyBuffer = SpanOwner<T>.Allocate(keyCount);
        var keySpan = keyBuffer.Span;
        for (var i = 0; i < keyCount; i++)
        {
            keySpan[i] = _serializer.Deserialize(ref reader);
        }

        // read ranges
        var rangeCount = (int)reader.ReadVarUInt32();
        var result = SpanOwner<T>.Allocate(rowCount);
        var span = result.Span;
        var added = 0;
        for (var r = 0; r < rangeCount; r++)
        {
            var index = (int)reader.ReadVarUInt32();
            var length = (int)reader.ReadVarUInt32();
            var value = keySpan[index];
            for (var i = 0; i < length; i++)
            {
                span[added++] = value;
            }
        }

        return result;
        */

        return MemoryOwner<T>.Empty;
    }

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, T value)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

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

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, int start, int length)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

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

    private static void VerifyEncodingId(ref Reader<SpanReaderInput> reader)
    {
        var id = reader.ReadVarUInt32();
        if (id != (int)WellKnownEncodings.Dictionary)
        {
            ThrowHelper.ThrowInvalidOperationException($"Payload does not start with the encoding marker of '{(int)WellKnownEncodings.Dictionary}'");
        }
    }

    private static int ReadRowCount(ref Reader<SpanReaderInput> reader) => (int)reader.ReadVarUInt32();
}