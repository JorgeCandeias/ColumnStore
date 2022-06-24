using BenchmarkDotNet.Attributes;
using CommunityToolkit.Diagnostics;
using CommunityToolkit.HighPerformance.Buffers;
using Orleans.Serialization.Buffers.Adaptors;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Outcompute.Benchmarks;

/// <summary>
/// This benchmark compares perf and allocs of various buffer writing methods to an unknown sized buffer.
/// The initial allocs from holding objects are included to represent column segment encoding behaviour.
/// Reading from or converting the buffer is not included in this benchmark.
/// </summary>
[MemoryDiagnoser, MarkdownExporter]
public class UnboundedByteBufferWriterBenchmarks
{
    private static readonly UnboundedChannelOptions _channelOptions = new() { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true };
    private static readonly PipeOptions _pipeOptions = new(null, PipeScheduler.Inline, PipeScheduler.Inline, -1, -1, -1, false);

    // one million values is the typical source column segment row count
    // it is also the worst case encoding scenario when plain encoding is elected
    [Params(1024 * 1024)]
    public int N { get; set; }

    [Benchmark]
    public void PooledArrayBufferWriter()
    {
        using var buffer = new PooledArrayBufferWriter(0);
        for (var i = 0; i < N; i++)
        {
            var span = buffer.GetSpan(1);
            span[0] = byte.MaxValue;
            buffer.Advance(1);
        }
    }

    [Benchmark]
    public void ArrayPoolBufferWriterBenchmark()
    {
        using var buffer = new ArrayPoolBufferWriter<byte>();
        for (var i = 0; i < N; i++)
        {
            var span = buffer.GetSpan(1);
            span[0] = byte.MaxValue;
            buffer.Advance(1);
        }
    }

    [Benchmark]
    public void ArrayBufferWriterBenchmark()
    {
        var buffer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < N; i++)
        {
            var span = buffer.GetSpan(1);
            span[0] = byte.MaxValue;
            buffer.Advance(1);
        }
    }

    [Benchmark]
    public void PipeBenchmark()
    {
        var pipe = new Pipe(_pipeOptions);
        for (var i = 0; i < N; i++)
        {
            var span = pipe.Writer.GetSpan(1);
            span[0] = byte.MaxValue;
            pipe.Writer.Advance(1);
        }
        pipe.Writer.Complete();
    }

    [Benchmark]
    public void ChannelBenchmark()
    {
        var channel = Channel.CreateUnbounded<byte>(_channelOptions);
        for (var i = 0; i < N; i++)
        {
            if (!channel.Writer.TryWrite(byte.MaxValue))
            {
                ThrowHelper.ThrowInvalidOperationException();
            }
        }
        channel.Writer.Complete();
    }
}