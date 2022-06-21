using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Outcompute.ColumnStore.ColumnSegments;
using Outcompute.ColumnStore.Segments;

namespace Outcompute.ColumnStore.Tests;

public class ColumnSegmentBuilderTests
{
    private readonly IServiceProvider _provider = new ServiceCollection()
        .AddSerializer()
        .AddColumnStore()
        .BuildServiceProvider();

    [Fact]
    public void Initializes()
    {
        // act
        var builder = _provider
            .GetRequiredService<ColumnSegmentBuilderFactory<int>>()
            .Create(Comparer<int>.Default);

        // assert
        Assert.Equal("", builder.Name);
        Assert.Equal(0, builder.Count);
    }

    [Fact]
    public void BuildsEmptyPrimitive()
    {
        // arrange
        var name = Guid.NewGuid().ToString();
        var builder = _provider
            .GetRequiredService<ColumnSegmentBuilderFactory<int>>()
            .Create(Comparer<int>.Default);

        // act
        builder.Name = name;
        var result = builder.ToImmutable();

        // assert
        Assert.Equal(0, builder.Count);
        Assert.IsType<ColumnSegment<int>>(result);
        Assert.Empty(result);
    }

    [Fact]
    public void BuildsSinglePrimitive()
    {
        // arrange
        var name = Guid.NewGuid().ToString();
        var builder = _provider
            .GetRequiredService<ColumnSegmentBuilderFactory<int>>()
            .Create(Comparer<int>.Default);

        // act
        builder.Name = name;
        builder.Add(123);
        var result = builder.ToImmutable();

        // assert
        Assert.Equal(1, builder.Count);
        Assert.Collection(result, x => Assert.Equal(123, x));
        Assert.Equal(name, result.Stats.Name);
        Assert.Equal(1, result.Stats.DistinctValueCount);
        Assert.Equal(0, result.Stats.DefaultValueCount);
    }

    [Fact]
    public void BuildsMultiPrimitive()
    {
        // arrange
        var name = Guid.NewGuid().ToString();
        var data = new[] { 123, 123, 0, 234, 0, 0, 234, 234, 345 };
        var builder = _provider
            .GetRequiredService<ColumnSegmentBuilderFactory<int>>()
            .Create(Comparer<int>.Default);

        // act
        builder.Name = name;
        foreach (var item in data)
        {
            builder.Add(item);
        }
        var result = builder.ToImmutable();

        // assert
        Assert.Equal(9, builder.Count);
        Assert.IsType<ColumnSegment<int>>(result);
        Assert.Collection(result, data.Select<int, Action<int>>((v, i) => v => Assert.Equal(data[i], v)).ToArray());

        Assert.Equal(name, result.Stats.Name);
        Assert.Equal(4, result.Stats.DistinctValueCount);
        Assert.Equal(3, result.Stats.DefaultValueCount);
    }
}