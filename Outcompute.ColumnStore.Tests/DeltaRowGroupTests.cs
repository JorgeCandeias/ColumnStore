using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;
using Outcompute.ColumnStore.Tests.ColumnStoreCodeGen;
using System.Buffers;

namespace Outcompute.ColumnStore.Tests;

public class DeltaRowGroupTests
{
    public const string CodeGenNamespace = "ColumnStoreCodeGen";

    private readonly IServiceProvider _provider;
    private readonly Type _generatedType;

    public DeltaRowGroupTests()
    {
        _provider = new ServiceCollection()
            .AddSerializer()
            .AddColumnStore()
            .BuildServiceProvider();

        _generatedType = FindType($"{typeof(DeltaRowGroupTests).Namespace}.{CodeGenNamespace}.{nameof(TestModel)}{typeof(DeltaRowGroup<>).Name.Replace("`1", "")}");
    }

    private TestModelDeltaRowGroup Create(int id, int capacity, params TestModel[] data)
    {
        var group = (TestModelDeltaRowGroup)_provider
            .GetRequiredService<IDeltaRowGroupFactory<TestModel>>()
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

    private static Type FindType(string name)
    {
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            if (assembly.GetType(name) is Type type)
            {
                return type;
            }
        }

        throw new InvalidOperationException($"Type '{name}' not found");
    }

    [Fact]
    public void Initializes()
    {
        // arrange
        var id = 123;

        //act
        var rows = Create(id, 1000);

        // assert empty state
        Assert.Equal(id, rows.Id);
        Assert.Equal(RowGroupState.Open, rows.State);
        Assert.Empty(rows);

        // assert empty stats
        Assert.Equal(id, rows.Stats.Id);
        Assert.Equal(0, rows.Stats.RowCount);
        Assert.Equal(5, rows.Stats.ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(nameof(TestModel.Prop1), rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].Name);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop2), rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].Name);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop3), rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].Name);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop4), rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].Name);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(nameof(TestModel.Prop5), rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].Name);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);

        // assert empty collection
        Assert.Empty(rows);
    }

    [Fact]
    public void AddsOne()
    {
        // arrange
        var rows = Create(123, 1000);

        // act
        foreach (var item in _data)
        {
            rows.Add(item);
        }

        // assert properties
        Assert.Equal(RowGroupState.Open, rows.State);
        Assert.Equal(6, rows.Count);

        // assert content
        foreach (var item in rows.Select((Model, Index) => (Model, Index)))
        {
            Assert.Equal(_data[item.Index] with { Ignored = false }, item.Model);
        }

        // assert stats
        Assert.Equal(6, rows.Stats.RowCount);
        Assert.Equal(5, rows.Stats.ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(6, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(2, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(4, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);
    }

    [Fact]
    public void AddsMany()
    {
        // arrange
        var rows = Create(123, 1000);

        // act
        rows.AddRange(_data);

        // assert properties
        Assert.Equal(RowGroupState.Open, rows.State);
        Assert.Equal(6, rows.Count);

        // assert content
        foreach (var item in rows.Select((Model, Index) => (Model, Index)))
        {
            Assert.Equal(_data[item.Index] with { Ignored = false }, item.Model);
        }

        // assert stats
        Assert.Equal(6, rows.Stats.RowCount);
        Assert.Equal(5, rows.Stats.ColumnSegmentStats.Count);

        // assert property stats
        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DefaultValueCount);
        Assert.Equal(6, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop1)].DistinctValueCount);

        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DefaultValueCount);
        Assert.Equal(2, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop2)].DistinctValueCount);

        Assert.Equal(0, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DefaultValueCount);
        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop3)].DistinctValueCount);

        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DefaultValueCount);
        Assert.Equal(4, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop4)].DistinctValueCount);

        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DefaultValueCount);
        Assert.Equal(3, rows.Stats.ColumnSegmentStats[nameof(TestModel.Prop5)].DistinctValueCount);
    }

    [Fact]
    public void RoundtripsViaConcreteSerializer()
    {
        // arrange
        var serializer = _provider.GetRequiredService<Serializer<TestModelDeltaRowGroup>>();
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
        Assert.True(input.GetReadOnlySequence().ToArray().SequenceEqual(output.GetReadOnlySequence().ToArray()));
    }

    [Fact]
    public void RoundtripsViaAbstractSerializer()
    {
        // arrange
        var inputSerializer = _provider.GetRequiredService<Serializer<TestModelDeltaRowGroup>>();
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
        Assert.True(input.GetReadOnlySequence().ToArray().SequenceEqual(output.GetReadOnlySequence().ToArray()));
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