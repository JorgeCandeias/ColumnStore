using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;

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
            .BuildServiceProvider();

        _generatedType = FindType($"{typeof(DeltaRowGroupTests).Namespace}.{CodeGenNamespace}.{nameof(TestModel)}{typeof(DeltaRowGroup<>).Name.Replace("`1", "")}");
    }

    private IDeltaRowGroup<TestModel> Create(int id) => (IDeltaRowGroup<TestModel>)ActivatorUtilities.CreateInstance(_provider, _generatedType, id);

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
        var rows = Create(id);

        // assert empty state
        Assert.Equal(id, rows.Id);
        Assert.Equal(RowGroupState.Open, rows.State);
        Assert.Equal(0, rows.Count);

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
        var rows = Create(123);

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
        var rows = Create(123);

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
    public void Serializes()
    {
        var rows = Create(123);
        rows.AddRange(_data);

        using var stream = new MemoryStream();

        var serializerType = typeof(Serializer<>).MakeGenericType(_generatedType);
        var serializer = _provider.GetRequiredService(serializerType);

        /*
        var serializer = _provider.GetRequiredService<Serializer>();
        var serialize = typeof(Serializer)
            .GetMethod(nameof(Serializer.Serialize), 1, new[] { Type.MakeGenericMethodParameter(0), typeof(Stream), typeof(int) })!
            .MakeGenericMethod(_generatedType);

        serialize.Invoke(serializer, new[] { rows, stream, Type.Missing });
        */

        //var serializer = _provider.GetRequiredService<Serializer<ColumnStoreCodeGen.TestModelDeltaRowGroup>>();
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