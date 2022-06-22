using System.Threading.Channels;

namespace Outcompute.ColumnStore.Core.Buffers;

public static class SpanOwnerExtensions
{
    public static SpanOwner<T> ToSpanOwner<T>(this ReadOnlySpan<T> source)
    {
        var owner = SpanOwner<T>.Allocate(source.Length);

        source.CopyTo(owner.Span);

        return owner;
    }

    public static SpanOwner<T> ToSpanOwner<T>(this Span<T> source)
    {
        var owner = SpanOwner<T>.Allocate(source.Length);

        source.CopyTo(owner.Span);

        return owner;
    }

    public static SpanOwner<T> ToSpanOwner<T>(this IEnumerable<T> source)
    {
        Guard.IsNotNull(source, nameof(source));

        if (source.TryGetNonEnumeratedCount(out var count))
        {
            return ToSpanOwnerFastPath(source, count);
        }
        else
        {
            return ToSpanOwnerSlowPath(source);
        }
    }

    private static SpanOwner<T> ToSpanOwnerFastPath<T>(IEnumerable<T> source, int count)
    {
        var owner = SpanOwner<T>.Allocate(count);
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

    private static SpanOwner<T> ToSpanOwnerSlowPath<T>(IEnumerable<T> source)
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

        var owner = SpanOwner<T>.Allocate(added);
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