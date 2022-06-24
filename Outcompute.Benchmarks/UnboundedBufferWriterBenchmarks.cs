using BenchmarkDotNet.Attributes;
using CommunityToolkit.Diagnostics;
using CommunityToolkit.HighPerformance.Buffers;
using Orleans.Serialization.Buffers.Adaptors;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Outcompute.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class UnboundedBufferWriterBenchmarks
{
    private readonly UnboundedChannelOptions _channelOptions = new() { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true };

    [Params(1_000_000)]
    public int N { get; set; }

    [Benchmark]
    public void PooledArrayBufferWriter()
    {
        using var buffer = new PooledArrayBufferWriter();
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
        var pipe = new Pipe();
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