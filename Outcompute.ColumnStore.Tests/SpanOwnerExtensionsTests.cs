using CommunityToolkit.HighPerformance;
using Outcompute.ColumnStore.Core.Buffers;

namespace Outcompute.ColumnStore.Tests;

public class SpanOwnerExtensionsTests
{
    [Fact]
    public void ToSpanOwnerFromReadOnlySpan()
    {
        // arrange
        ReadOnlySpan<int> source = new int[] { 1, 2, 3 };

        // act
        using var owner = source.ToSpanOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.SequenceEqual(owner.Span));
    }

    [Fact]
    public void ToSpanOwnerFromSpan()
    {
        // arrange
        Span<int> source = new int[] { 1, 2, 3 };

        // act
        using var owner = source.ToSpanOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.SequenceEqual(owner.Span));
    }

    [Fact]
    public void ToSpanOwnerFastPathEmpty()
    {
        // arrange
        var source = new List<int>();

        // act
        using var owner = source.ToSpanOwner();

        // assert
        Assert.Equal(0, owner.Span.Length);
    }

    [Fact]
    public void ToSpanOwnerFastPathFilled()
    {
        // arrange
        var source = new List<int>() { 1, 2, 3 };

        // act
        using var owner = source.ToSpanOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.AsSpan().SequenceEqual(owner.Span));
    }

    [Fact]
    public void ToSpanOwnerSlowPathEmpty()
    {
        // arrange
        static IEnumerable<int> Source()
        {
            yield break;
        }
        var source = Source();

        // act
        using var owner = source.ToSpanOwner();

        // assert
        Assert.Equal(0, owner.Span.Length);
    }

    [Fact]
    public void ToSpanOwnerSlowPathFilled()
    {
        // arrange
        static IEnumerable<int> Source()
        {
            yield return 1;
            yield return 2;
            yield return 3;
        }
        var source = Source();

        // act
        using var owner = source.ToSpanOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.ToList().AsSpan().SequenceEqual(owner.Span));
    }
}