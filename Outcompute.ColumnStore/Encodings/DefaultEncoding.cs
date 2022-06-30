using Microsoft.Extensions.Options;
using Orleans.Serialization.Buffers;

namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// The default encoding implementation for when no other encoding is available.
/// This encoding serializes each sequence value individually and works with complex types.
/// This often generates the largest payload of all encodings but it can result in a smaller one for sequences with very high cardinality.
/// </summary>
internal sealed class DefaultEncoding<T> : Encoding<T>
{
    private readonly Serializer<T> _serializer;
    private readonly SerializerSessionPool _sessions;
    private readonly EncodingOptions _options;

    public DefaultEncoding(Serializer<T> serializer, SerializerSessionPool sessions, IOptions<EncodingOptions> options)
    {
        Guard.IsNotNull(serializer, nameof(serializer));
        Guard.IsNotNull(sessions, nameof(sessions));
        Guard.IsNotNull(options, nameof(options));

        _serializer = serializer;
        _sessions = sessions;
        _options = options.Value;
    }

    public override void Encode<TBufferWriter>(ReadOnlySpan<T> source, TBufferWriter bufferWriter)
    {
        // arrange
        using var session = _sessions.GetSession();
        var writer = Writer.Create(bufferWriter, session);

        // write top headers
        writer.WriteEncodingId(WellKnownEncodings.Default);
        writer.WriteCount(source.Length);

        // enumerate each value in the sequence
        using var buffer = new ArrayPoolBufferWriter<byte>();
        for (var i = 0; i < source.Length; i++)
        {
            // serialize the value upfront so we know the payload size
            buffer.Clear();
            _serializer.Serialize(source[i], buffer);

            // write value headers to allow read skipping
            writer.WriteHash(buffer.WrittenSpan);
            writer.WriteCount(buffer.WrittenCount);

            // write value
            writer.Write(buffer.WrittenSpan);
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
            // ignore headers
            reader.ReadHash();
            reader.ReadCount();

            // read the value
            span[i] = _serializer.Deserialize(ref reader);
        }

        return buffer;
    }

    public override IMemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, T value)
    {
        // calculate the stable hash of the value being searched
        var hash = ComputeHash(value);

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
            // read current value headers
            var currHash = reader.ReadHash();
            var currSize = reader.ReadCount();

            // if the hashes are different then skip deserializing altogether
            if (currHash != hash)
            {
                reader.Skip(currSize);
                continue;
            }

            // if the values are different then continue looping
            var candidate = _serializer.Deserialize(ref reader);
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
                // read next value headers
                var nextHash = reader.ReadHash();
                var nextSize = reader.ReadCount();

                // if the hash is different then skip reading and stop the scan
                if (nextHash != currHash)
                {
                    reader.Skip(nextSize);
                    break;
                }

                // if the value is different then stop the scan
                var next = _serializer.Deserialize(ref reader);
                if (!comparer.Equals(value, next))
                {
                    break;
                }

                // otherwise increase the range
                length++;
            }

            // yield the found range
            var span = buffer.GetSpan(1);
            span[0] = new ValueRange<T>(value, start, length);
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
            reader.ReadHash();
            var size = reader.ReadCount();
            reader.Skip(size);
        }

        // consume all ranges until reaching length
        var comparer = EqualityComparer<T>.Default;
        var end = start + length;
        if (start < end)
        {
            var current = _serializer.Deserialize(ref reader);
            var s = start;
            var l = 1;

            for (var i = start + 1; i < end; i++)
            {
                var next = _serializer.Deserialize(ref reader);
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

    private uint ComputeHash(T value)
    {
        using var temp = SpanOwner<byte>.Allocate(_options.ValueBufferSize);
        var span = temp.Span;

        _serializer.Serialize(value, ref span);

        return JenkinsHash.ComputeHash(span);
    }
}