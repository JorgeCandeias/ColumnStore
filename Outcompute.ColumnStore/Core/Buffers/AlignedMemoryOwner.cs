using System.Runtime.InteropServices;

namespace Outcompute.ColumnStore.Core.Buffers;

/// <summary>
/// An <see cref="IMemoryOwner{T}"/> implementation that allocates native aligned memory.
/// It is capable of resizing the underlying buffer up and down but not slicing.
/// </summary>
internal sealed class AlignedMemoryOwner<T> : MemoryManager<T>
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
    /// The sliced length of the buffer.
    /// </summary>
    private int _length;

    /// <summary>
    /// The alignment of the buffer;
    /// </summary>
    private int _alignment;

    /// <summary>
    /// Used for interlocked disposed checking.
    /// </summary>
    private int _disposed = 0;

    private unsafe AlignedMemoryOwner(byte* memory, int length, int alignment)
    {
        _memory = memory;
        _length = length;
        _alignment = alignment;
    }

    public override unsafe Span<T> GetSpan()
    {
        return new(_memory, _length);
    }

    public override unsafe MemoryHandle Pin(int elementIndex = 0)
    {
        // validation here accounts for slicing
        Guard.IsGreaterThanOrEqualTo(elementIndex, 0, nameof(elementIndex));
        Guard.IsLessThan(elementIndex, _length, nameof(elementIndex));

        // no need to pin native memory as the gc cannot move it so just return a dummy handle
        return new(_memory + elementIndex, default, this);
    }

    public override void Unpin()
    {
        // no need to unpin native memory
    }

    /// <summary>
    /// Resizes the underlying memory buffer up or down and/or changes its alignment.
    /// </summary>
    /// <param name="length">The new length of the buffer.</param>
    /// <param name="clear">Whether to clear the newly allocated area of the buffer.</param>
    /// <param name="alignment">The new alignment of the buffer.</param>
    public unsafe void Resize(int length, bool clear = false, int alignment = DefaultAlignment)
    {
        Guard.IsGreaterThanOrEqualTo(length, 0, nameof(length));
        Guard.IsGreaterThan(alignment, 0, nameof(alignment));

        // noop if nothing will change
        if (length == _length && alignment == _alignment)
        {
            return;
        }

        // compute new byte length
        var size = sizeof(T);
        var bytes = length * size;

        // resize the native memory block
        _memory = (byte*)NativeMemory.AlignedRealloc(_memory, (uint)bytes, (uint)alignment);

        // clear the new bytes if request
        if (clear && length > _length)
        {
            var span = new Span<T>(_memory, length);

            span[_length..].Clear();
        }

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

        // done
        _length = length;
        _alignment = alignment;
    }

    protected override unsafe void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
        {
            return;
        }

        NativeMemory.AlignedFree(_memory);

        GC.RemoveMemoryPressure(_length);
    }

    public static unsafe AlignedMemoryOwner<T> Allocate(int length, int alignment = 64)
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

        // create a new owner for the user to release the buffer
        return new AlignedMemoryOwner<T>(memory, length, alignment);
    }
}