// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.DependencyInjection;
using Outcompute.ColumnStore;

var provider = new ServiceCollection()
    .AddColumnStore()
    .BuildServiceProvider();

var factory = provider
    .GetRequiredService<IColumnStoreFactory<TestModel>>();

var count = 100_000_000;

TestList(count);
TestColumnStore(factory, count);

static void TestList(int count)
{
    var before = GC.GetTotalMemory(true);
    var list = new List<TestModel>();

    foreach (var item in Generate(count))
    {
        list.Add(item);
    }

    var after = GC.GetTotalMemory(true);
    var diff = after - before;

    Console.WriteLine($"List using {diff} bytes");

    GC.KeepAlive(list);
}

static void TestColumnStore(IColumnStoreFactory<TestModel> factory, int count)
{
    var before = GC.GetTotalMemory(true);
    var cs = factory.Create();

    foreach (var item in Generate(count))
    {
        cs.Add(item);
    }

    var after = GC.GetTotalMemory(true);
    var diff = after - before;

    Console.WriteLine($"ColumnStore using {diff} bytes");

    GC.KeepAlive(cs);
}

static IEnumerable<TestModel> Generate(int count)
{
    for (var i = 0; i < count; i++)
    {
        yield return new TestModel
        {
            Year = i / 100,
            Category = (i / 1000).ToString()
        };
    }
}

[ColumnStore]
public record struct TestModel()
{
    [ColumnStoreProperty]
    public int Year { get; set; } = 0;

    [ColumnStoreProperty]
    public string Category { get; set; } = string.Empty;
}