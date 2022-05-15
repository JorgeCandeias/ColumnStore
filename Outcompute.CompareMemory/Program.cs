// See https://aka.ms/new-console-template for more information
using Outcompute.ColumnStore;

var count = 100_000_000;

TestList(count);
TestColumnStore(count);

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

    // keep the list collection referenced
    list.ToString();
}

static void TestColumnStore(int count)
{
    var before = GC.GetTotalMemory(true);
    var cs = new ColumnStore<TestModel>();

    foreach (var item in Generate(count))
    {
        cs.Add(item);
    }

    var after = GC.GetTotalMemory(true);
    var diff = after - before;

    Console.WriteLine($"ColumnStore using {diff} bytes");

    // keep the collection referenced
    cs.ToString();
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