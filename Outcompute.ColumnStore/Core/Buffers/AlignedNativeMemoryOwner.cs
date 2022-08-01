using System.Runtime.InteropServices;

namespace Outcompute.ColumnStore.Core.Buffers;

/// <summary>
/// An <see cref="IMemoryOwner{T}"/> implementation that allocates native aligned memory.
/// It is capable of resizing the underlying buffer up and down but not slicing yet.
/// </summary>
internal sealed class AlignedNativeMemoryOwner<T> : MemoryManager<T>
    where T : unmanaged
{
    /// <summary>
    /// The default memory alignment.
    /// </summary>
    public const int DefaultAlignment = 64;

    /// <summary>
    /// Pointer to the native memory buffer.
    /// </summary>
    private unsafe byte* _memory;

    /// <summary>
    /// The length of the buffer.
    /// </summary>
    private int _length;

    /// <summary>
    /// The alignment of the buffer;
    /// </summary>
    private readonly int _alignment;

    /// <summary>
    /// Used for the interlocked dispose check.
    /// </summary>
    private int _disposed = 0;

    private unsafe AlignedNativeMemoryOwner(byte* memory, int length, int alignment)
    {
        _memory = memory;
        _length = length;
        _alignment = alignment;
    }

    /// <summary>
    /// The current length of the buffer.
    /// </summary>
    public int Length => _length;

    public override unsafe Span<T> GetSpan()
    {
        return new(_memory, _length);
    }

    public override unsafe MemoryHandle Pin(int elementIndex = 0)
    {
        Guard.IsGreaterThanOrEqualTo(elementIndex, 0, nameof(elementIndex));
        Guard.IsLessThan(elementIndex, _length, nameof(elementIndex));

        // no need to pin native memory as the gc cannot move it so just return a plain handle
        return new(_memory + elementIndex, default, this);
    }

    public override void Unpin()
    {
        // no need to unpin native memory
    }

    /// <summary>
    /// Resizes the underlying memory buffer up or down.
    /// </summary>
    /// <param name="length">The new length of the buffer.</param>
    /// <param name="clear">Whether to clear the newly allocated portion of the buffer when enlarging it.</param>
    public unsafe void Resize(int length, bool clear = false)
    {
        Guard.IsGreaterThanOrEqualTo(length, 0, nameof(length));

        // noop if nothing will change
        if (length == _length)
        {
            return;
        }

        // calculate new byte length
        var size = sizeof(T);
        var bytes = length * size;

        // resize the native memory block
        _memory = (byte*)NativeMemory.AlignedRealloc(_memory, (uint)bytes, (uint)_alignment);

        // adjust memory pressure
        var diff = length - _length;
        if (diff > 0)
        {
            GC.AddMemoryPressure(diff);
        }
        else if (diff < 0)
        {
            GC.RemoveMemoryPressure(-diff);
        }

        // clear the new bytes if requested and applicable
        if (clear && length > _length)
        {
            var span = new Span<T>(_memory, length);

            span[_length..].Clear();
        }

        // done
        _length = length;
    }

    protected override unsafe void Dispose(bool disposing)
    {
        // this guarantees the buffer is only released once
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
        {
            return;
        }

        // now it is safe to release the buffer
        NativeMemory.AlignedFree(_memory);
        GC.RemoveMemoryPressure(_length);
    }

    public static unsafe AlignedNativeMemoryOwner<T> Allocate(int length, bool clear = false, int alignment = DefaultAlignment)
    {
        Guard.IsGreaterThanOrEqualTo(length, 0, nameof(length));
        Guard.IsGreaterThan(alignment, 0, nameof(alignment));

        // compute byte length
        var size = sizeof(T);
        var bytes = length * size;

        // grab the memory block
        byte* memory = (byte*)NativeMemory.AlignedAlloc((uint)bytes, (uint)alignment);

        // adjust memory pressure
        GC.AddMemoryPressure(length);

        // clear the new bytes if requested
        if (clear)
        {
            var span = new Span<T>(memory, length);

            span.Clear();
        }

        // create a new owner for the user to release the buffer
        return new AlignedNativeMemoryOwner<T>(memory, length, alignment);
    }
}