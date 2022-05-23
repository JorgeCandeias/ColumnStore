using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;
using Outcompute.ColumnStore;
using Serilog;
using XPTO;

var logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

// detailed collection statistics
var cs = new ColumnStore<Book>(5);

cs.Add(new Book { Year = 1, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 1, Title = "B" });
PrintStats();

cs.Add(new Book { Year = 1, Title = "C" });
PrintStats();

cs.Add(new Book { Year = 2, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 2, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "A" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "B" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "C" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "C" });
PrintStats();

cs.Add(new Book { Year = 3, Title = "C" });
PrintStats();

// serializer payload comparison
var provider = new ServiceCollection()
    .AddSerializer()
    .BuildServiceProvider();

var serializer = provider.GetRequiredService<Serializer>();

MeasureMemory<object?>(() => null);
MeasureMemory(() => new object());
MeasureMemory(() => new EmptyClass());
MeasureMemory(() => new EmptyRecord());
MeasureMemory(() => new Range1(1));
MeasureMemory(() => new Range2(1, 1000));
MeasureMemory(() => new Range2N(1, null));
MeasureMemory(() => new Range2N(1, 1000));
MeasureMemory(() => new Range1L(1));
MeasureMemory(() => new Range2L(1, 1000));
MeasureMemory(() => new Range2LN(1, null));
MeasureMemory(() => new Range2LN(1, 1000));

MeasureMemory(() => new[] { new Range1(1), new Range1(1000) });

/*
var list = new List<Book>();

var listExp = Expression.Constant(list);

Expression<Func<Book, int>> yearLambda = (Book x) => x.Year;

var qqq = (Book x) => x.Year;

foreach (var prop in new[] { "Year", "Title" })
{
    var order1 = Expression.Call(listExp, typeof(Enumerable).GetMethod(nameof(Enumerable.OrderBy))!, );
}

*/

void MeasureMemory<T>(Func<T> create)
{
    T data = default!;
    long memory = 0;
    byte[] bytes = Array.Empty<byte>();
    long allocSerialize = 0;
    long allocDeserialize = 0;

    // repeat a few times to stabilize serializer buffers and keep the last results
    for (var i = 0; i < 100; i++)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        data = create();
        var after = GC.GetAllocatedBytesForCurrentThread();
        memory = after - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        bytes = serializer.SerializeToArray(data);
        var after1 = GC.GetAllocatedBytesForCurrentThread();
        var temp = new byte[bytes.Length];
        var after2 = GC.GetAllocatedBytesForCurrentThread();
        var discard = (after2 - after1) * 2;
        allocSerialize = after1 - before - discard;

        before = GC.GetAllocatedBytesForCurrentThread();
        serializer.Deserialize<T>(bytes);
        after = GC.GetAllocatedBytesForCurrentThread();
        allocDeserialize = after - before - memory;
    }

    logger.Information(
        "{@Item} is {Memory} bytes in-memory, {Payload} bytes serialized, {AllocSerialized} garbage bytes serializing, {AllocDeserialized} garbage bytes deserializing",
        data,
        memory,
        bytes.Length,
        allocSerialize,
        allocDeserialize);
}

void PrintStats()
{
    foreach (var item in cs)
    {
        logger.Information("{@Item}", item);
    }
    logger.Information("{@Stats}", cs.GetStats());
    logger.Information("");
}

namespace XPTO
{
    [ColumnStore]
    public record class Book()
    {
        [ColumnStoreProperty(typeof(ReverseComparer<>))]
        public int Year { get; set; } = 0;

        [ColumnStoreProperty]
        public string Title { get; set; } = "";
    }

    public class ReverseComparer<T> : Comparer<T>
    {
        public override int Compare(T? x, T? y)
        {
            return -Default.Compare(x, y);
        }
    }

    [GenerateSerializer]
    public class EmptyClass
    { };

    [GenerateSerializer]
    public record class EmptyRecord();

    [GenerateSerializer]
    public record class Range1([property: Id(1)] int Index);

    [GenerateSerializer]
    public record class Range2([property: Id(1)] int Start, [property: Id(2)] int End);

    [GenerateSerializer]
    public record class Range2N([property: Id(1)] int Start, [property: Id(2)] int? End);

    [GenerateSerializer]
    public record class Range1L([property: Id(1)] long Index);

    [GenerateSerializer]
    public record class Range2L([property: Id(1)] long Start, [property: Id(2)] long End);

    [GenerateSerializer]
    public record class Range2LN([property: Id(1)] long Start, [property: Id(2)] long? End);
}