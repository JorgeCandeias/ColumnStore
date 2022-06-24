using BenchmarkDotNet.Attributes;
using CommunityToolkit.HighPerformance.Buffers;
using Microsoft.Extensions.ObjectPool;
using Orleans.Serialization.Buffers;
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
[MemoryDiagnoser, MarkdownExporter, ShortRunJob]
public class UnboundedByteBufferWriterBenchmarks
{
    private static readonly UnboundedChannelOptions _channelOptions = new() { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true };
    private static readonly PipeOptions _pipeOptions = new(null, PipeScheduler.Inline, PipeScheduler.Inline, -1, -1, -1, false);

    // one million values is the typical source column segment row count
    // it is also the worst case encoding scenario when plain encoding is elected
    [Params(1024 * 1024)]
    public int N { get; set; }

    // number of items written in each go
    // this helps identify how write frequency affects perf on each case
    [Params(1, 1024)]
    public int Written { get; set; }

    [Benchmark]
    public void PooledArrayBufferWriterLatest()
    {
        using var buffer = new PooledArrayBufferWriterLatest(0);
        for (var i = 0; i < N; i += Written)
        {
            var span = buffer.GetSpan(Written);
            span[..Written].Fill(byte.MaxValue);
            buffer.Advance(Written);
        }
    }

    [Benchmark]
    public void PooledArrayBufferWriter()
    {
        using var buffer = new PooledArrayBufferWriter(0);
        for (var i = 0; i < N; i += Written)
        {
            var span = buffer.GetSpan(Written);
            span[..Written].Fill(byte.MaxValue);
            buffer.Advance(Written);
        }
    }

    [Benchmark]
    public void ArrayPoolBufferWriter()
    {
        using var buffer = new ArrayPoolBufferWriter<byte>();
        for (var i = 0; i < N; i += Written)
        {
            var span = buffer.GetSpan(Written);
            span[..Written].Fill(byte.MaxValue);
            buffer.Advance(Written);
        }
    }

    [Benchmark]
    public void ArrayBufferWriter()
    {
        var buffer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < N; i += Written)
        {
            var span = buffer.GetSpan(Written);
            span[..Written].Fill(byte.MaxValue);
            buffer.Advance(Written);
        }
    }

    [Benchmark]
    public void Pipe()
    {
        var pipe = new Pipe(_pipeOptions);
        for (var i = 0; i < N; i += Written)
        {
            var span = pipe.Writer.GetSpan(Written);
            span[..Written].Fill(byte.MaxValue);
            pipe.Writer.Advance(Written);
        }
        pipe.Writer.Complete();
    }

    /*
    [Benchmark]
    public void Channel()
    {
        var channel = System.Threading.Channels.Channel.CreateUnbounded<byte>(_channelOptions);
        for (var i = 0; i < N; i++)
        {
            if (!channel.Writer.TryWrite(byte.MaxValue))
            {
                ThrowHelper.ThrowInvalidOperationException();
            }
        }
        channel.Writer.Complete();
    }
    */
}

/// <summary>
/// A <see cref="IBufferWriter{T}"/> implementation implemented using pooled arrays.
/// </summary>
public struct PooledArrayBufferWriterLatest : IBufferWriter<byte>, IDisposable
{
    private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;
    private readonly List<(byte[] Array, int Length)> _committed;
    private readonly int _minAllocationSize;
    private byte[] _current;
    private long _totalLength;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> struct.
    /// </summary>
    public PooledArrayBufferWriterLatest() : this(0)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> struct.
    /// </summary>
    /// <param name="minAllocationSize">Minimum size of the allocation.</param>
    public PooledArrayBufferWriterLatest(int minAllocationSize)
    {
        _committed = new();
        _current = Array.Empty<byte>();
        _totalLength = 0;
        _minAllocationSize = minAllocationSize > 0 ? minAllocationSize : 4096;
    }

    /// <summary>Gets the total length which has been written.</summary>
    public readonly long Length => _totalLength;

    /// <summary>
    /// Returns the data which has been written as an array.
    /// </summary>
    /// <returns>The data which has been written.</returns>
    public readonly byte[] ToArray()
    {
        var result = new byte[_totalLength];
        var resultSpan = result.AsSpan();
        foreach (var (buffer, length) in _committed)
        {
            buffer.AsSpan(0, length).CopyTo(resultSpan);
            resultSpan = resultSpan.Slice(length);
        }

        return result;
    }

    /// <inheritdoc/>
    public void Advance(int bytes)
    {
        if (bytes == 0)
        {
            return;
        }

        _committed.Add((_current, bytes));
        _totalLength += bytes;
        _current = Array.Empty<byte>();
    }

    public void Reset()
    {
        foreach (var (array, _) in _committed)
        {
            if (array.Length == 0)
            {
                continue;
            }

            Pool.Return(array);
        }

        _committed.Clear();
        _current = Array.Empty<byte>();
        _totalLength = 0;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Reset();
    }

    /// <inheritdoc/>
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (sizeHint == 0)
        {
            sizeHint = _current.Length + _minAllocationSize;
        }

        if (sizeHint < _current.Length)
        {
            throw new InvalidOperationException("Attempted to allocate a new buffer when the existing buffer has sufficient free space.");
        }

        var newBuffer = Pool.Rent(Math.Max(sizeHint, _minAllocationSize));
        _current.CopyTo(newBuffer.AsSpan());
        Pool.Return(_current);
        _current = newBuffer;
        return newBuffer;
    }

    /// <inheritdoc/>
    public Span<byte> GetSpan(int sizeHint)
    {
        if (sizeHint == 0)
        {
            sizeHint = _current.Length + _minAllocationSize;
        }

        if (sizeHint < _current.Length)
        {
            throw new InvalidOperationException("Attempted to allocate a new buffer when the existing buffer has sufficient free space.");
        }

        var newBuffer = Pool.Rent(Math.Max(sizeHint, _minAllocationSize));
        _current.CopyTo(newBuffer.AsSpan());
        Pool.Return(_current);
        _current = newBuffer;
        return newBuffer;
    }

    /// <summary>Copies the contents of this writer to another writer.</summary>
    public readonly void CopyTo<TBufferWriter>(ref Writer<TBufferWriter> writer) where TBufferWriter : IBufferWriter<byte>
    {
        foreach (var (buffer, length) in _committed)
        {
            writer.Write(buffer.AsSpan(0, length));
        }
    }

    /// <summary>
    /// Returns a new <see cref="ReadOnlySequence{T}"/> which must be used and returned before resetting this instance via the <see cref="ReturnReadOnlySequence"/> method.
    /// </summary>
    public readonly ReadOnlySequence<byte> RentReadOnlySequence()
    {
        if (_totalLength == 0)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        if (_committed.Count == 1)
        {
            var value = _committed[0];
            return new ReadOnlySequence<byte>(value.Array, 0, value.Length);
        }

        var runningIndex = 0;
        var firstSegment = default(BufferSegment);
        var previousSegment = default(BufferSegment);
        foreach (var (buffer, length) in _committed)
        {
            var segment = BufferSegment.Pool.Get();
            segment.Initialize(new ReadOnlyMemory<byte>(buffer, 0, length), runningIndex);

            runningIndex += length;

            previousSegment?.SetNext(segment);

            firstSegment ??= segment;
            previousSegment = segment;
        }

        return new ReadOnlySequence<byte>(firstSegment, 0, previousSegment, previousSegment.Memory.Length);
    }

    /// <summary>
    /// Returns a <see cref="ReadOnlySequence{T}"/> previously rented by <see cref="RentReadOnlySequence"/>;
    /// </summary>
    public readonly void ReturnReadOnlySequence(in ReadOnlySequence<byte> sequence)
    {
        if (sequence.Start.GetObject() is not BufferSegment segment)
        {
            return;
        }

        while (segment is not null)
        {
            var next = segment.Next as BufferSegment;
            BufferSegment.Pool.Return(segment);
            segment = next;
        }
    }
}

internal sealed class BufferSegment : ReadOnlySequenceSegment<byte>
{
    public static readonly ObjectPool<BufferSegment> Pool = ObjectPool.Create(new SegmentPoolPolicy());

    public void Initialize(ReadOnlyMemory<byte> memory, long runningIndex)
    {
        Memory = memory;
        RunningIndex = runningIndex;
    }

    public void SetNext(BufferSegment next) => Next = next;

    public void Reset()
    {
        Memory = default;
        RunningIndex = default;
        Next = default;
    }

    private sealed class SegmentPoolPolicy : PooledObjectPolicy<BufferSegment>
    {
        public override BufferSegment Create() => new();

        public override bool Return(BufferSegment obj)
        {
            obj.Reset();
            return true;
        }
    }
}

/// <summary>
/// A <see cref="IBufferWriter{T}"/> implementation implemented using pooled arrays.
/// </summary>
public struct PooledArrayBufferWriterHacked : IBufferWriter<byte>, IDisposable
{
    private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;
    private readonly List<(byte[] Array, int Length)> _committed;
    private readonly int _minAllocationSize;
    private byte[] _current;
    private long _totalLength;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> struct.
    /// </summary>
    public PooledArrayBufferWriterHacked() : this(0)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledArrayBufferWriter"/> struct.
    /// </summary>
    /// <param name="minAllocationSize">Minimum size of the allocation.</param>
    public PooledArrayBufferWriterHacked(int minAllocationSize)
    {
        _committed = new();
        _current = Array.Empty<byte>();
        _totalLength = 0;
        _minAllocationSize = minAllocationSize > 0 ? minAllocationSize : 4096;
    }

    /// <summary>Gets the total length which has been written.</summary>
    public readonly long Length => _totalLength;

    /// <summary>
    /// Returns the data which has been written as an array.
    /// </summary>
    /// <returns>The data which has been written.</returns>
    public readonly byte[] ToArray()
    {
        var result = new byte[_totalLength];
        var resultSpan = result.AsSpan();
        foreach (var (buffer, length) in _committed)
        {
            buffer.AsSpan(0, length).CopyTo(resultSpan);
            resultSpan = resultSpan.Slice(length);
        }

        return result;
    }

    /// <inheritdoc/>
    public void Advance(int bytes)
    {
        if (bytes == 0)
        {
            return;
        }

        _committed.Add((_current, bytes));
        _totalLength += bytes;
        _current = Array.Empty<byte>();
    }

    public void Reset()
    {
        foreach (var (array, _) in _committed)
        {
            if (array.Length == 0)
            {
                continue;
            }

            Pool.Return(array);
        }

        _committed.Clear();
        _current = Array.Empty<byte>();
        _totalLength = 0;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Reset();
    }

    /// <inheritdoc/>
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (sizeHint == 0)
        {
            sizeHint = _current.Length + _minAllocationSize;
        }

        if (sizeHint < _current.Length)
        {
            throw new InvalidOperationException("Attempted to allocate a new buffer when the existing buffer has sufficient free space.");
        }

        var newBuffer = Pool.Rent(Math.Max(sizeHint, _minAllocationSize));
        _current.CopyTo(newBuffer.AsSpan());
        Pool.Return(_current);
        _current = newBuffer;
        return newBuffer;
    }

    /// <inheritdoc/>
    public Span<byte> GetSpan(int sizeHint)
    {
        if (sizeHint == 0)
        {
            sizeHint = _current.Length + _minAllocationSize;
        }

        if (sizeHint < _current.Length)
        {
            throw new InvalidOperationException("Attempted to allocate a new buffer when the existing buffer has sufficient free space.");
        }

        var newBuffer = Pool.Rent(Math.Max(sizeHint, _minAllocationSize));
        _current.CopyTo(newBuffer.AsSpan());
        Pool.Return(_current);
        _current = newBuffer;
        return newBuffer;
    }

    /// <summary>Copies the contents of this writer to another writer.</summary>
    public readonly void CopyTo<TBufferWriter>(ref Writer<TBufferWriter> writer) where TBufferWriter : IBufferWriter<byte>
    {
        foreach (var (buffer, length) in _committed)
        {
            writer.Write(buffer.AsSpan(0, length));
        }
    }

    /// <summary>
    /// Returns a new <see cref="ReadOnlySequence{T}"/> which must be used and returned before resetting this instance via the <see cref="ReturnReadOnlySequence"/> method.
    /// </summary>
    public readonly ReadOnlySequence<byte> RentReadOnlySequence()
    {
        if (_totalLength == 0)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        if (_committed.Count == 1)
        {
            var value = _committed[0];
            return new ReadOnlySequence<byte>(value.Array, 0, value.Length);
        }

        var runningIndex = 0;
        var firstSegment = default(BufferSegment);
        var previousSegment = default(BufferSegment);
        foreach (var (buffer, length) in _committed)
        {
            var segment = BufferSegment.Pool.Get();
            segment.Initialize(new ReadOnlyMemory<byte>(buffer, 0, length), runningIndex);

            runningIndex += length;

            previousSegment?.SetNext(segment);

            firstSegment ??= segment;
            previousSegment = segment;
        }

        return new ReadOnlySequence<byte>(firstSegment, 0, previousSegment, previousSegment.Memory.Length);
    }

    /// <summary>
    /// Returns a <see cref="ReadOnlySequence{T}"/> previously rented by <see cref="RentReadOnlySequence"/>;
    /// </summary>
    public readonly void ReturnReadOnlySequence(in ReadOnlySequence<byte> sequence)
    {
        if (sequence.Start.GetObject() is not BufferSegment segment)
        {
            return;
        }

        while (segment is not null)
        {
            var next = segment.Next as BufferSegment;
            BufferSegment.Pool.Return(segment);
            segment = next;
        }
    }
}