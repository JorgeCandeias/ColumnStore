namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// A basic sequential encoding base appropriate for small unmanaged types such as numeric primitives.
/// This encoding serializes each sequence value individually without headers.
/// This encoding approach is very inefficient for heap based types of any size and so its usage is disabled for those.
/// </summary>
internal abstract class SequentialEncoding<T> : Encoding<T>
    where T : unmanaged
{
    private readonly SerializerSessionPool _sessions;

    protected SequentialEncoding(SerializerSessionPool sessions)
    {
        Guard.IsNotNull(sessions, nameof(sessions));

        _sessions = sessions;
    }

    protected abstract WellKnownEncodings EncodingId { get; }

    protected abstract void Serialize<TBufferWriter>(ref Writer<TBufferWriter> writer, T value)
        where TBufferWriter : IBufferWriter<byte>;

    protected abstract T Deserialize<TInput>(ref Reader<TInput> reader);

    public override void Encode<TBufferWriter>(ReadOnlySpan<T> source, TBufferWriter bufferWriter)
    {
        // arrange
        using var session = _sessions.GetSession();
        var writer = Writer.Create(bufferWriter, session);

        // write top headers
        writer.WriteEncodingId(WellKnownEncodings.Default);
        writer.WriteCount(source.Length);

        // enumerate each value in the sequence
        for (var i = 0; i < source.Length; i++)
        {
            Serialize(ref writer, source[i]);
        }

        writer.Commit();
    }

    public override IMemoryOwner<T> Decode(ReadOnlySpan<byte> source)
    {
        // create writing artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

        // read top headers
        reader.VerifyEncodingId(WellKnownEncodings.Default);
        var count = reader.ReadCount();

        // read each value
        var buffer = MemoryOwner<T>.Allocate(count);
        var span = buffer.Span;
        for (var i = 0; i < count; i++)
        {
            span[i] = Deserialize(ref reader);
        }

        return buffer;
    }

    public override IMemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, T value)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

        // read top headers
        reader.VerifyEncodingId(WellKnownEncodings.Default);
        int count = reader.ReadCount();

        // we dont know the potential result count so use a dynamic buffer
        var buffer = new ArrayPoolBufferWriter<ValueRange<T>>();

        // look for a range start
        var comparer = EqualityComparer<T>.Default;
        for (var i = 0; i < count; i++)
        {
            // if the values are different then continue looping
            var candidate = Deserialize(ref reader);
            if (!comparer.Equals(candidate, value))
            {
                continue;
            }

            // initialize the range
            var start = i;
            var length = 1;

            // look for a range end
            for (i++; i < count; i++)
            {
                // if the value is different then stop the scan
                var next = Deserialize(ref reader);
                if (!comparer.Equals(value, next))
                {
                    break;
                }

                // otherwise increase the range
                length++;
            }

            // yield the found range
            buffer.GetSpan(1)[0] = new ValueRange<T>(value, start, length);
            buffer.Advance(1);
        }

        return buffer;
    }

    public override IMemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, int start, int length)
    {
        Guard.IsGreaterThanOrEqualTo(start, 0, nameof(start));
        Guard.IsGreaterThanOrEqualTo(length, 0, nameof(length));

        // break early if the window is empty
        if (length is 0)
        {
            return MemoryOwner<ValueRange<T>>.Empty;
        }

        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

        // read top headers
        reader.VerifyEncodingId(WellKnownEncodings.Default);
        var count = reader.ReadCount();

        // break early if there is nothing to read
        if (count is 0)
        {
            return MemoryOwner<ValueRange<T>>.Empty;
        }

        // we dont know the potential result count so use a dynamic buffer
        var buffer = new ArrayPoolBufferWriter<ValueRange<T>>();

        // skip data until window start
        for (var i = 0; i < start; i++)
        {
            _ = Deserialize(ref reader);
        }

        // consume all ranges until reaching length
        var comparer = EqualityComparer<T>.Default;
        var end = start + length;
        if (start < end)
        {
            var current = Deserialize(ref reader);
            var s = start;
            var l = 1;

            for (var i = start + 1; i < end; i++)
            {
                var next = Deserialize(ref reader);
                if (comparer.Equals(current, next))
                {
                    l++;
                }
                else
                {
                    // close the interim range
                    buffer.GetSpan(1)[0] = new ValueRange<T>(current, s, l);
                    buffer.Advance(1);

                    current = next;
                    s = i;
                    l = 1;
                }
            }

            // close the final range
            buffer.GetSpan(1)[0] = new ValueRange<T>(current, s, l);
            buffer.Advance(1);
        }

        // return a buffer sliced to its contents
        return buffer;
    }
}