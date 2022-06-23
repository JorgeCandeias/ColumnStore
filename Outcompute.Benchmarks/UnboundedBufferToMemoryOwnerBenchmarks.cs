using BenchmarkDotNet.Attributes;
using CommunityToolkit.Diagnostics;
using CommunityToolkit.HighPerformance.Buffers;
using Outcompute.ColumnStore.Core.Buffers;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Outcompute.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class UnboundedBufferToMemoryOwnerBenchmarks
{
    private readonly UnboundedChannelOptions _channelOptions = new() { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true };

    [Params(100_000, 1_000_000, 10_000_000)]
    public int N { get; set; }

    [Benchmark]
    public IMemoryOwner<byte> ArrayPoolBufferWriterBenchmark()
    {
        var buffer = new ArrayPoolBufferWriter<byte>();
        for (var i = 0; i < N; i++)
        {
            var span = buffer.GetSpan(1);
            span[0] = byte.MaxValue;
            buffer.Advance(1);
        }
        return buffer;
    }

    [Benchmark]
    public IMemoryOwner<byte> PipeBenchmark()
    {
        var pipe = new Pipe();
        for (var i = 0; i < N; i++)
        {
            var span = pipe.Writer.GetSpan(1);
            span[0] = byte.MaxValue;
            pipe.Writer.Advance(1);
        }
        pipe.Writer.Complete();

        return pipe.Reader.ToMemoryOwner(true);
    }

    [Benchmark]
    public IMemoryOwner<byte> ChannelBenchmark()
    {
        var channel = Channel.CreateUnbounded<byte>(_channelOptions);
        var added = 0;
        for (var i = 0; i < N; i++)
        {
            if (!channel.Writer.TryWrite(byte.MaxValue))
            {
                ThrowHelper.ThrowInvalidOperationException();
            }
            added++;
        }
        channel.Writer.Complete();

        var owner = MemoryOwner<byte>.Allocate(added);
        var span = owner.Span;
        for (var i = 0; i < added; i++)
        {
            if (!channel.Reader.TryRead(out var item))
            {
                ThrowHelper.ThrowInvalidOperationException();
            }
            span[i] = item;
        }
        return owner;
    }
}