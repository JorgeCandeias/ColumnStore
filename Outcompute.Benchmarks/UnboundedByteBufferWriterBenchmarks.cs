using BenchmarkDotNet.Attributes;
using CommunityToolkit.HighPerformance.Buffers;
using CommunityToolkit.HighPerformance.Helpers;
using Orleans.Serialization.Buffers.Adaptors;
using System.Buffers;
using System.IO.Pipelines;

namespace Outcompute.Benchmarks;

/// <summary>
/// This benchmark compares perf and allocs of various buffer writing methods to an unknown sized buffer.
/// The initial allocs from holding objects are included to represent column segment encoding behaviour.
/// Reading from or converting the buffer is not included in this benchmark.
/// </summary>
[MemoryDiagnoser, MarkdownExporter, ShortRunJob]
public class UnboundedByteBufferWriterBenchmarks
{
    private static readonly PipeOptions _pipeOptions = new(null, PipeScheduler.Inline, PipeScheduler.Inline, -1, -1, -1, false);

    // 1M values is the typical source column segment row count
    // it is also the worst case encoding scenario when plain encoding is elected
    // it is also the max shared array pool size before it starts allocating fixed sized arrays
    [Params(1024, 1024 * 1024)]
    public int N { get; set; }

    // number of items written in each go
    // this helps identify how write frequency affects perf on each case based on requested write buffer
    [Params(1, 1024)]
    public int Written { get; set; }

    // number of parallel workers
    // simulates how the rowgroup conversion process will encode multiple columns in parallel
    [Params(1, 2, 4)]
    public int Parallelism { get; set; }

    public readonly struct PooledArrayBufferWriterLatestWorker : IAction
    {
        private readonly int _n;
        private readonly int _written;

        public PooledArrayBufferWriterLatestWorker(int n, int written)
        {
            _n = n;
            _written = written;
        }

        public void Invoke(int i)
        {
            using var buffer = new PooledArrayBufferWriterLatest(0);
            for (var x = 0; x < _n; x += _written)
            {
                var span = buffer.GetSpan(_written);
                span[.._written].Fill(byte.MaxValue);
                buffer.Advance(_written);
            }
        }
    }

    [Benchmark]
    public void PooledArrayBufferWriterLatest()
    {
        ParallelHelper.For(0..Parallelism, new PooledArrayBufferWriterLatestWorker(N, Written));
    }

    public readonly struct PooledArrayBufferWriterWorker : IAction
    {
        private readonly int _n;
        private readonly int _written;

        public PooledArrayBufferWriterWorker(int n, int written)
        {
            _n = n;
            _written = written;
        }

        public void Invoke(int i)
        {
            using var buffer = new PooledArrayBufferWriter(0);
            for (var x = 0; x < _n; x += _written)
            {
                var span = buffer.GetSpan(_written);
                span[.._written].Fill(byte.MaxValue);
                buffer.Advance(_written);
            }
        }
    }

    [Benchmark]
    public void PooledArrayBufferWriter()
    {
        ParallelHelper.For(0..Parallelism, new PooledArrayBufferWriterWorker(N, Written));
    }

    public readonly struct ArrayPoolBufferWriterWorker : IAction
    {
        private readonly int _n;
        private readonly int _written;

        public ArrayPoolBufferWriterWorker(int n, int written)
        {
            _n = n;
            _written = written;
        }

        public void Invoke(int i)
        {
            using var buffer = new ArrayPoolBufferWriter<byte>();
            for (var x = 0; x < _n; x += _written)
            {
                var span = buffer.GetSpan(_written);
                span[.._written].Fill(byte.MaxValue);
                buffer.Advance(_written);
            }
        }
    }

    [Benchmark]
    public void ArrayPoolBufferWriter()
    {
        ParallelHelper.For(0..Parallelism, new ArrayPoolBufferWriterWorker(N, Written));
    }

    public readonly struct ArrayBufferWriterWorker : IAction
    {
        private readonly int _n;
        private readonly int _written;

        public ArrayBufferWriterWorker(int n, int written)
        {
            _n = n;
            _written = written;
        }

        public void Invoke(int i)
        {
            var buffer = new ArrayBufferWriter<byte>();
            for (var x = 0; x < _n; x += _written)
            {
                var span = buffer.GetSpan(_written);
                span[.._written].Fill(byte.MaxValue);
                buffer.Advance(_written);
            }
        }
    }

    [Benchmark]
    public void ArrayBufferWriter()
    {
        ParallelHelper.For(0..Parallelism, new ArrayBufferWriterWorker(N, Written));
    }

    public readonly struct PipeWorker : IAction
    {
        private readonly int _n;
        private readonly int _written;

        public PipeWorker(int n, int written)
        {
            _n = n;
            _written = written;
        }

        public void Invoke(int i)
        {
            var pipe = new Pipe(_pipeOptions);
            for (var x = 0; x < _n; x += _written)
            {
                var span = pipe.Writer.GetSpan(_written);
                span[.._written].Fill(byte.MaxValue);
                pipe.Writer.Advance(_written);
            }
            pipe.Writer.Complete();
        }
    }

    [Benchmark]
    public void Pipe()
    {
        ParallelHelper.For(0..Parallelism, new PipeWorker(N, Written));
    }
}