using System.Threading.Channels;

namespace Outcompute.ColumnStore.Core.Buffers;

public static class SpanOwnerExtensions
{
    public static SpanOwner<T> ToSpanOwner<T>(this ReadOnlySpan<T> source)
    {
        var owner = SpanOwner<T>.Allocate(source.Length);

        try
        {
            source.CopyTo(owner.Span);
        }
        catch (ArgumentException)
        {
            owner.Dispose();
            throw;
        }

        return owner;
    }

    public static SpanOwner<T> ToSpanOwner<T>(this Span<T> source)
    {
        var owner = SpanOwner<T>.Allocate(source.Length);

        try
        {
            source.CopyTo(owner.Span);
        }
        catch (ArgumentException)
        {
            owner.Dispose();
            throw;
        }

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

        return owner;
    }

    private static SpanOwner<T> ToSpanOwnerSlowPath<T>(IEnumerable<T> source)
    {
        var owner = SpanOwner<T>.Allocate(1024);
        var added = 0;

        foreach (var item in source)
        {
            if (added == owner.Length)
            {
                var bigger = SpanOwner<T>.Allocate(owner.Length * 2);
                owner.Span.CopyTo(bigger.Span);
                owner.Dispose();
                owner = bigger;
            }

            owner.Span[added++] = item;
        }

        return owner;
    }
}