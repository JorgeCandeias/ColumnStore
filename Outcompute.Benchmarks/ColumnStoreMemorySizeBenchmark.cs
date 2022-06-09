using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Outcompute.ColumnStore;

namespace Outcompute.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class ColumnStoreMemorySizeBenchmark
{
    private readonly IColumnStoreFactory<TestModel> _factory;

    public ColumnStoreMemorySizeBenchmark()
    {
        var provider = new ServiceCollection()
            .AddColumnStore()
            .BuildServiceProvider();

        _factory = provider.GetRequiredService<IColumnStoreFactory<TestModel>>();
    }

    private TestModel[] _data = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = new TestModel[N];

        for (var i = 0; i < N; i++)
        {
            _data[i] = new TestModel
            {
                Year = i / 100,
                Category = (i / 1000).ToString()
            };
        }
    }

    [Params(100)]
    public int N { get; set; }

    [Benchmark(Baseline = true)]
    public void List()
    {
        var list = new List<TestModel>();

        for (var i = 0; i < _data.Length; i++)
        {
            list.Add(_data[i]);
        }
    }

    [Benchmark]
    public void ColumnStore()
    {
        var cs = _factory.Create();

        for (var i = 0; i < _data.Length; i++)
        {
            cs.Add(_data[i]);
        }
    }
}

[ColumnStore]
public record TestModel()
{
    [ColumnStoreProperty]
    public int Year { get; set; } = 0;

    [ColumnStoreProperty]
    public string Category { get; set; } = string.Empty;
}