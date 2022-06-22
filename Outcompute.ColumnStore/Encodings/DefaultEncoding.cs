﻿using Orleans.Serialization.Buffers;
using System.IO.Pipelines;

namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// The default encoding implementation for when no other encoding is available.
/// This encoding serializes each sequence value individually.
/// This often generates the largest payload but it can result in a smaller one for sequences with very high cardinality.
/// </summary>
internal sealed class DefaultEncoding<T> : Encoding<T>
{
    private readonly Serializer<T> _serializer;
    private readonly SerializerSessionPool _sessions;

    public DefaultEncoding(Serializer<T> serializer, SerializerSessionPool sessions)
    {
        Guard.IsNotNull(serializer, nameof(serializer));
        Guard.IsNotNull(sessions, nameof(sessions));

        _serializer = serializer;
        _sessions = sessions;
    }

    public override MemoryOwner<byte> Encode(ReadOnlySpan<T> source)
    {
        // create writing artefacts
        using var session = _sessions.GetSession();
        var pipe = new Pipe();
        var writer = Writer.Create(pipe.Writer, session);

        // prefix with the encoding id
        writer.WriteVarUInt32((uint)WellKnownEncodings.Default);

        // prefix with the value count
        writer.WriteVarUInt32((uint)source.Length);

        // write each value
        foreach (var value in source)
        {
            _serializer.Serialize(value, ref writer);
        }

        // convert to contiguous buffer
        writer.Commit();
        pipe.Writer.Complete();
        if (!pipe.Reader.TryRead(out var data))
        {
            ThrowHelper.ThrowInvalidOperationException();
        }

        var result = MemoryOwner<byte>.Allocate((int)data.Buffer.Length);
        data.Buffer.CopyTo(result.Span);
        pipe.Reader.Complete();

        return result;
    }

    public override MemoryOwner<T> Decode(ReadOnlySpan<byte> source)
    {
        // create writing artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

        // read the encoding id
        var id = reader.ReadVarUInt32();
        if (id != (int)WellKnownEncodings.Default)
        {
            ThrowHelper.ThrowInvalidOperationException($"Payload does not start with the encoding marker of '{(int)WellKnownEncodings.Dictionary}'");
        }

        // read the value count prefix
        var count = (int)reader.ReadVarUInt32();

        // read each value into the output buffer
        var buffer = MemoryOwner<T>.Allocate(count, AllocationMode.Clear);
        for (var i = 0; i < count; i++)
        {
            buffer.Span[i] = _serializer.Deserialize(ref reader);
        }
        return buffer;
    }

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, T value)
    {
        // create reading artefacts
        using var session = _sessions.GetSession();
        var reader = Reader.Create(source, session);

        // read the encoding id
        var id = reader.ReadVarUInt32();
        if (id != (int)WellKnownEncodings.Default)
        {
            ThrowHelper.ThrowInvalidOperationException($"Payload does not start with the encoding marker of '{(int)WellKnownEncodings.Dictionary}'");
        }

        // read the value count prefix
        var count = (int)reader.ReadVarUInt32();

        // break early if there is nothing to read
        if (count == 0)
        {
            return MemoryOwner<ValueRange<T>>.Empty;
        }

        // allocate a pessimistic buffer assuming max cardinality
        var buffer = MemoryOwner<ValueRange<T>>.Allocate(count, AllocationMode.Clear);
        var added = 0;

        // look for a range start
        var comparer = EqualityComparer<T>.Default;
        for (var i = 0; i < count; i++)
        {
            var candidate = _serializer.Deserialize(ref reader);
            if (comparer.Equals(value, candidate))
            {
                // initialize the range
                var start = i;
                var length = 1;

                // look for a range end
                for (; i < count; i++)
                {
                    var next = _serializer.Deserialize(ref reader);
                    if (comparer.Equals(value, next))
                    {
                        length++;
                    }
                    else
                    {
                        break;
                    }
                }

                // yield the found range
                buffer.Span[added++] = new ValueRange<T>(value, start, length);
            }
        }

        // return a buffer sliced to its contents
        return buffer[..added];
    }

    public override MemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, int start, int length)
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

        // read the encoding id
        var id = reader.ReadVarUInt32();
        if (id != (int)WellKnownEncodings.Default)
        {
            ThrowHelper.ThrowInvalidOperationException($"Payload does not start with the encoding marker of '{(int)WellKnownEncodings.Dictionary}'");
        }

        // read the value count prefix
        var count = (int)reader.ReadVarUInt32();

        // break early if there is nothing to read
        if (count is 0)
        {
            return MemoryOwner<ValueRange<T>>.Empty;
        }

        // allocate a buffer with at most the window size - worst case scenario of max cardinality
        var buffer = MemoryOwner<ValueRange<T>>.Allocate(length, AllocationMode.Clear);
        var added = 0;

        // consume data until the window start
        for (var i = 0; i < start; i++)
        {
            _serializer.Deserialize(ref reader);
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
                    buffer.Span[added++] = new ValueRange<T>(current, s, l);
                    current = next;
                    s = i;
                    l = 1;
                }
            }

            // close the final range
            buffer.Span[added++] = new ValueRange<T>(current, s, l);
        }

        // return a buffer sliced to its contents
        return buffer[..added];
    }
}