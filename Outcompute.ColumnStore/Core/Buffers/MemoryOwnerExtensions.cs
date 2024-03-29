﻿using System.IO.Pipelines;
using System.Threading.Channels;

namespace Outcompute.ColumnStore.Core.Buffers;

public static class MemoryOwnerExtensions
{
    /// <summary>
    /// Creates a <see cref="MemoryOwner{T}"/> from the contents of the specified <see cref="PipeReader"/>.
    /// Optionally completes the specified <see cref="PipeReader"/> for convenience.
    /// </summary>
    public static MemoryOwner<byte> ToMemoryOwner(this PipeReader reader, bool complete = false)
    {
        Guard.IsNotNull(reader, nameof(reader));

        MemoryOwner<byte> owner;

        if (reader.TryRead(out var result))
        {
            var sequence = result.Buffer;
            owner = MemoryOwner<byte>.Allocate((int)sequence.Length);
            sequence.CopyTo(owner.Span);
        }
        else
        {
            owner = MemoryOwner<byte>.Empty;
        }

        if (complete)
        {
            reader.Complete();
        }

        return owner;
    }

    public static MemoryOwner<T> ToMemoryOwner<T>(this ReadOnlySpan<T> source)
    {
        var owner = MemoryOwner<T>.Allocate(source.Length);

        source.CopyTo(owner.Span);

        return owner;
    }

    public static MemoryOwner<T> ToMemoryOwner<T>(this Span<T> source)
    {
        var owner = MemoryOwner<T>.Allocate(source.Length);

        source.CopyTo(owner.Span);

        return owner;
    }

    public static MemoryOwner<T> ToMemoryOwner<T>(this IEnumerable<T> source)
    {
        Guard.IsNotNull(source, nameof(source));

        if (source.TryGetNonEnumeratedCount(out var count))
        {
            return ToMemoryOwnerFastPath(source, count);
        }
        else
        {
            return ToMemoryOwnerSlowPath(source);
        }
    }

    private static MemoryOwner<T> ToMemoryOwnerFastPath<T>(IEnumerable<T> source, int count)
    {
        var owner = MemoryOwner<T>.Allocate(count);
        var span = owner.Span;
        var added = 0;

        foreach (var item in source)
        {
            span[added++] = item;
        }

        if (added != count)
        {
            ThrowHelper.ThrowInvalidOperationException($"Enumerated count of {added} differs from announced count of {count}");
        }

        return owner;
    }

    private static readonly UnboundedChannelOptions _options = new()
    {
        SingleReader = true,
        SingleWriter = true,
        AllowSynchronousContinuations = true
    };

    private static MemoryOwner<T> ToMemoryOwnerSlowPath<T>(IEnumerable<T> source)
    {
        var channel = Channel.CreateUnbounded<T>(_options);
        var added = 0;

        foreach (var item in source)
        {
            if (channel.Writer.TryWrite(item))
            {
                added++;
            }
            else
            {
                ThrowHelper.ThrowInvalidOperationException();
            }
        }
        channel.Writer.Complete();

        var owner = MemoryOwner<T>.Allocate(added);
        var span = owner.Span;
        for (var i = 0; i < added; i++)
        {
            if (channel.Reader.TryRead(out var item))
            {
                span[i] = item;
            }
            else
            {
                ThrowHelper.ThrowInvalidOperationException();
            }
        }

        return owner;
    }
}