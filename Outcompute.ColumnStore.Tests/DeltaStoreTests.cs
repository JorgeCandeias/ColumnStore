using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;

namespace Outcompute.ColumnStore.Tests;

public class DeltaStoreTests
{
    private readonly IServiceProvider _provider;

    public DeltaStoreTests()
    {
        _provider = new ServiceCollection()
            .AddSerializer()
            .AddColumnStore()
            .BuildServiceProvider();
    }

    private DeltaStore<TestModel> Create(int rowGroupCapacity, params TestModel[] data)
    {
        var store = _provider
            .GetRequiredService<DeltaStoreFactory<TestModel>>()
            .Create(rowGroupCapacity);

        if (data.Length > 0)
        {
            store.AddRange(data);
        }

        return store;
    }

    private readonly TestModel[] _data = new[]
    {
        new TestModel(1, "A", 100.1M, 1.1, null, true),
        new TestModel(2, "A", 100.1M, null, "AAA", true),
        new TestModel(3, "A", 100.2M, 2.2, null, true),
        new TestModel(4, "B", 100.2M, null, "BBB", true),
        new TestModel(5, "B", 100.3M, 3.3, null, true),
        new TestModel(6, "B", 100.3M, null, "BBB", true)
    };

    [Fact]
    public void Initializes()
    {
        //act
        var store = Create(1000);

        // assert empty state
        Assert.Empty(store);
        Assert.Equal(1000, store.RowGroupCapacity);

        // assert empty stats
        Assert.Equal(0, store.Stats.RowCount);
        Assert.Equal(0, store.Stats.RowGroupStats.Count);
    }

    [Fact]
    public void AddsOne()
    {
        // arrange
        var store = Create(1000);

        // act
        foreach (var item in _data)
        {
            store.Add(item);
        }

        // assert properties
        Assert.Equal(6, store.Count);

        // assert content
        foreach (var item in store.Select((Model, Index) => (Model, Index)))
        {
            Assert.Equal(_data[item.Index] with { Ignored = false }, item.Model);
        }

        // assert stats
        Assert.Equal(6, store.Stats.RowCount);
        Assert.Equal(1, store.Stats.RowGroupStats.Count);
        Assert.Equal(5, store.Stats.RowGroupStats[0].ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(0, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(6, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(0, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(2, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(0, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(4, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);
    }

    [Fact]
    public void AddsMany()
    {
        // arrange
        var store = Create(1000);

        // act
        store.AddRange(_data);

        // assert properties
        Assert.Equal(6, store.Count);

        // assert content
        foreach (var item in store.Select((Model, Index) => (Model, Index)))
        {
            Assert.Equal(_data[item.Index] with { Ignored = false }, item.Model);
        }

        // assert stats
        Assert.Equal(6, store.Stats.RowCount);
        Assert.Equal(1, store.Stats.RowGroupStats.Count);
        Assert.Equal(5, store.Stats.RowGroupStats[0].ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(0, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(6, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(0, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(2, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(0, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(4, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(3, store.Stats.RowGroupStats[0].ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);
    }

    [Fact]
    public void RoundtripsViaConcreteSerializer()
    {
        // arrange
        var serializer = _provider.GetRequiredService<Serializer<DeltaStore<TestModel>>>();
        var input = Create(1000, _data);

        // act - serialize
        using var stream = new MemoryStream();
        serializer.Serialize(input, stream, 0);

        // act - deserialize
        stream.Position = 0;
        var output = serializer.Deserialize(stream);

        // assert
        Assert.NotNull(output);
        Assert.Equal(input.Count, output.Count);
        Assert.Equal(input.RowGroupCapacity, output.RowGroupCapacity);
        Assert.Equal(input.Stats.RowCount, output.Stats.RowCount);
        Assert.Equal(input.Stats.RowGroupStats.Count, output.Stats.RowGroupStats.Count);

        foreach (var item in input.Stats.RowGroupStats.Values)
        {
            var other = output.Stats.RowGroupStats[item.Id];

            Assert.Equal(item.Id, other.Id);
            Assert.Equal(item.RowCount, other.RowCount);
            Assert.Equal(item.ColumnSegmentStats.Count, other.ColumnSegmentStats.Count);

            foreach (var col in item.ColumnSegmentStats.Values)
            {
                var otherCol = other.ColumnSegmentStats[col.Name];

                Assert.Equal(col.Name, otherCol.Name);
                Assert.Equal(col.DefaultValueCount, otherCol.DefaultValueCount);
                Assert.Equal(col.DistinctValueCount, otherCol.DistinctValueCount);
            }
        }
    }

    [Id(1001)]
    [GenerateSerializer, ColumnStore]
    public record struct TestModel(
        [property: Id(1), ColumnStoreProperty] int Prop1,
        [property: Id(2), ColumnStoreProperty] string Prop2,
        [property: Id(3), ColumnStoreProperty] decimal Prop3,
        [property: Id(4), ColumnStoreProperty] double? Prop4,
        [property: Id(5), ColumnStoreProperty] string? Prop5,
        bool Ignored);
}