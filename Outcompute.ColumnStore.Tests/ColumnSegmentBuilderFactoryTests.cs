using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

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
        Assert.IsType<ColumnSegmentBuilder<int>>(builder);
        Assert.Equal("", builder.Name);
        Assert.Equal(0, builder.Count);
    }
}