using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Outcompute.ColumnStore.ColumnSegments;

namespace Outcompute.ColumnStore.Tests;

public class ColumnSegmentBuilderFactoryTests
{
    private readonly IServiceProvider _provider = new ServiceCollection()
        .AddSerializer()
        .AddColumnStore()
        .BuildServiceProvider();

    [Fact]
    public void Creates()
    {
        // arrange
        var factory = _provider.GetRequiredService<ColumnSegmentBuilderFactory<int>>();

        // act
        var builder = factory.Create(Comparer<int>.Default);

        // assert
        Assert.Equal("", builder.Name);
        Assert.Equal(0, builder.Count);
    }
}