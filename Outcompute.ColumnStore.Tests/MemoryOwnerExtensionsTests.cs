using CommunityToolkit.HighPerformance;
using Outcompute.ColumnStore.Core.Buffers;
using System.Buffers;
using System.IO.Pipelines;

namespace Outcompute.ColumnStore.Tests;

public class MemoryOwnerExtensionsTests
{
    [Fact]
    public void ToMemoryOwnerFromEmptyPipeReader()
    {
        // arrange
        var pipe = new Pipe();

        // act
        using var owner = pipe.Reader.ToMemoryOwner(true);

        // assert
        Assert.Equal(0, owner.Span.Length);
    }

    [Fact]
    public void ToMemoryOwnerFromFilledPipeReader()
    {
        // arrange
        var data = new byte[] { 1, 2, 3 };
        var pipe = new Pipe();
        foreach (var item in data)
        {
            pipe.Writer.Write(item);
        }
        pipe.Writer.Complete();

        // act
        using var owner = pipe.Reader.ToMemoryOwner(true);

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(owner.Span.SequenceEqual(data.AsSpan()));
    }

    [Fact]
    public void ToMemoryOwnerFromReadOnlySpan()
    {
        // arrange
        ReadOnlySpan<int> source = new int[] { 1, 2, 3 };

        // act
        using var owner = source.ToMemoryOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.SequenceEqual(owner.Span));
    }

    [Fact]
    public void ToMemoryOwnerFromSpan()
    {
        // arrange
        Span<int> source = new int[] { 1, 2, 3 };

        // act
        using var owner = source.ToMemoryOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.SequenceEqual(owner.Span));
    }

    [Fact]
    public void ToMemoryOwnerFastPathEmpty()
    {
        // arrange
        var source = new List<int>();

        // act
        using var owner = source.ToMemoryOwner();

        // assert
        Assert.Equal(0, owner.Span.Length);
    }

    [Fact]
    public void ToMemoryOwnerFastPathFilled()
    {
        // arrange
        var source = new List<int>() { 1, 2, 3 };

        // act
        using var owner = source.ToMemoryOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.AsSpan().SequenceEqual(owner.Span));
    }

    [Fact]
    public void ToMemoryOwnerSlowPathEmpty()
    {
        // arrange
        static IEnumerable<int> Source()
        {
            yield break;
        }
        var source = Source();

        // act
        using var owner = source.ToMemoryOwner();

        // assert
        Assert.Equal(0, owner.Span.Length);
    }

    [Fact]
    public void ToMemoryOwnerSlowPathFilled()
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
        using var owner = source.ToMemoryOwner();

        // assert
        Assert.Equal(3, owner.Span.Length);
        Assert.True(source.ToList().AsSpan().SequenceEqual(owner.Span));
    }
}