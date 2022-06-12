using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;
using System.Buffers;
using TestDeltaRowGroup = Outcompute.ColumnStore.Tests.ColumnStoreCodeGen.DeltaRowGroupTestsTestModelDeltaRowGroup;

namespace Outcompute.ColumnStore.Tests;

public class DeltaRowGroupTests
{
    private readonly IServiceProvider _provider;

    public DeltaRowGroupTests()
    {
        _provider = new ServiceCollection()
            .AddSerializer()
            .AddColumnStore()
            .BuildServiceProvider();
    }

    private TestDeltaRowGroup Create(int id, int capacity, params TestModel[] data)
    {
        var group = (TestDeltaRowGroup)_provider
            .GetRequiredService<DeltaRowGroupFactory<TestModel>>()
            .Create(id, capacity);

        group.AddRange(data);

        return group;
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
        // arrange
        var id = 123;
        var capacity = 1000;

        //act
        var group = Create(id, capacity);

        // assert empty state
        Assert.Equal(id, group.Id);
        Assert.Equal(RowGroupState.Open, group.State);
        Assert.Equal(capacity, group.Capacity);
        Assert.Equal(0, group.Version);
        Assert.Equal(0, group.GetReadOnlySequence().Length);
        Assert.Empty(group);

        // assert empty stats
        Assert.Equal(id, group.Stats.Id);
        Assert.Equal(0, group.Stats.RowCount);
        Assert.Equal(5, group.Stats.ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(nameof(TestModel.Prop1), group.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].Name);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop2), group.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].Name);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop3), group.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].Name);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop4), group.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].Name);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop5), group.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].Name);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);

        // assert empty collection
        Assert.Empty(group);
    }

    [Fact]
    public void AddsOne()
    {
        // arrange
        var group = Create(123, 1000);

        // act
        foreach (var item in _data)
        {
            group.Add(item);
        }

        // assert properties
        Assert.Equal(RowGroupState.Open, group.State);
        Assert.Equal(6, group.Version);
        Assert.Equal(6, group.Count);
        Assert.NotEqual(0, group.GetReadOnlySequence().Length);

        // assert content
        foreach (var item in group.Select((Model, Index) => (Model, Index)))
        {
            Assert.Equal(_data[item.Index] with { Ignored = false }, item.Model);
        }

        // assert stats
        Assert.Equal(6, group.Stats.RowCount);
        Assert.Equal(5, group.Stats.ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(6, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(2, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(4, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);
    }

    [Fact]
    public void AddsMany()
    {
        // arrange
        var group = Create(123, 1000);

        // act
        group.AddRange(_data);

        // assert properties
        Assert.Equal(RowGroupState.Open, group.State);
        Assert.Equal(1, group.Version);
        Assert.Equal(6, group.Count);
        Assert.NotEqual(0, group.GetReadOnlySequence().Length);

        // assert content
        foreach (var item in group.Select((Model, Index) => (Model, Index)))
        {
            Assert.Equal(_data[item.Index] with { Ignored = false }, item.Model);
        }

        // assert stats
        Assert.Equal(6, group.Stats.RowCount);
        Assert.Equal(5, group.Stats.ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(6, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(2, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(0, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(4, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(3, group.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);
    }

    [Fact]
    public void RoundtripsViaConcreteSerializer()
    {
        // arrange
        var serializer = _provider.GetRequiredService<Serializer<TestDeltaRowGroup>>();
        var input = Create(123, 1000, _data);

        // act - serialize
        using var stream = new MemoryStream();
        serializer.Serialize(input, stream, 0);

        // act - deserialize
        stream.Position = 0;
        var output = serializer.Deserialize(stream);

        // assert
        Assert.NotNull(output);
        Assert.Equal(input.Id, output.Id);
        Assert.Equal(input.Capacity, output.Capacity);
        Assert.Equal(input.State, output.State);
        Assert.Equal(input.Count, output.Count);
        Assert.Equal(input.Version, output.Version);

        Assert.Equal(input.Stats.Id, output.Stats.Id);
        Assert.Equal(input.Stats.RowCount, output.Stats.RowCount);
        Assert.Equal(input.Stats.ColumnSegmentStats.Count, output.Stats.ColumnSegmentStats.Count);

        foreach (var item in input.Stats.ColumnSegmentStats)
        {
            var other = output.Stats.ColumnSegmentStats[item.Key];

            Assert.Equal(item.Value.Name, other.Name);
            Assert.Equal(item.Value.DistinctValueCount, other.DistinctValueCount);
            Assert.Equal(item.Value.DefaultValueCount, other.DefaultValueCount);
        }

        Assert.True(input.GetReadOnlySequence().ToArray().SequenceEqual(output.GetReadOnlySequence().ToArray()));
    }

    [Fact]
    public void RoundtripsViaAbstractSerializer()
    {
        // arrange
        var inputSerializer = _provider.GetRequiredService<Serializer<DeltaRowGroup<TestModel>>>();
        var input = Create(123, 1000, _data);
        var outputSerializer = _provider.GetRequiredService<Serializer<DeltaRowGroup<TestModel>>>();

        // act - serialize
        using var stream = new MemoryStream();
        inputSerializer.Serialize(input, stream, 0);

        // act - deserialize
        stream.Position = 0;
        var output = outputSerializer.Deserialize(stream);

        // assert
        Assert.NotNull(output);
        Assert.Equal(input.Id, output.Id);
        Assert.Equal(input.Capacity, output.Capacity);
        Assert.Equal(input.State, output.State);
        Assert.Equal(input.Count, output.Count);
        Assert.Equal(input.Version, output.Version);

        Assert.Equal(input.Stats.Id, output.Stats.Id);
        Assert.Equal(input.Stats.RowCount, output.Stats.RowCount);
        Assert.Equal(input.Stats.ColumnSegmentStats.Count, output.Stats.ColumnSegmentStats.Count);

        foreach (var item in input.Stats.ColumnSegmentStats)
        {
            var other = output.Stats.ColumnSegmentStats[item.Key];

            Assert.Equal(item.Value.Name, other.Name);
            Assert.Equal(item.Value.DistinctValueCount, other.DistinctValueCount);
            Assert.Equal(item.Value.DefaultValueCount, other.DefaultValueCount);
        }

        Assert.True(input.GetReadOnlySequence().ToArray().SequenceEqual(output.GetReadOnlySequence().ToArray()));
    }

    [GenerateSerializer, ColumnStore]
    public record struct TestModel(
        [property: Id(1), ColumnStoreProperty] int Prop1,
        [property: Id(2), ColumnStoreProperty] string Prop2,
        [property: Id(3), ColumnStoreProperty] decimal Prop3,
        [property: Id(4), ColumnStoreProperty] double? Prop4,
        [property: Id(5), ColumnStoreProperty] string? Prop5,
        bool Ignored);
}