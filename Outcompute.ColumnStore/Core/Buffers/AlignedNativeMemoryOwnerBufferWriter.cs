using System.Numerics;

namespace Outcompute.ColumnStore.Core.Buffers;

/// <summary>
/// A buffer writer over an underlying <see cref="AlignedNativeMemoryOwner{T}"/>.
/// This writer will resize the underlying buffer as required.
/// </summary>
internal sealed class AlignedNativeMemoryOwnerBufferWriter<T> : IBuffer<T>, IDisposable
    where T : unmanaged
{
    /// <summary>
    /// The default initial capacity of the buffer.
    /// There is no reasoning as of yet behind this default.
    /// For ideal performance, the user should select an initial capacity as close as possible to the content length.
    /// </summary>
    public const int DefaultInitialCapacity = 1024;

    /// <summary>
    /// The underlying buffer managed by this writer.
    /// </summary>
    private readonly AlignedNativeMemoryOwner<T> _owner;

    /// <summary>
    /// The next writing position, also doubles as the count of written items.
    /// </summary>
    private int _index;

    /// <summary>
    /// Whether to resize the buffer on overflow.
    /// </summary>
    private readonly bool _resize;

    /// <summary>
    /// Whether to clear new portions of the buffer when auto-resizing.
    /// Enable this when advancing past written portions of the buffer to avoid acquiring garbage data.
    /// Otherwise leave disabled to maximize resizing performance.
    /// </summary>
    private readonly bool _clear;

    /// <summary>
    /// Whether to dispose the buffer when this writer instance is disposed.
    /// </summary>
    private readonly bool _dispose;

    /// <summary>
    /// Best effort to avoid disposing multiple times.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Creates an instance of <see cref="AlignedNativeMemoryOwnerBufferWriter{T}"/> over an internal <see cref="AlignedNativeMemoryOwner{T}"/> instance.
    /// </summary>
    /// <param name="initialLength">The initial capacity of the underlying buffer.</param>
    /// <param name="alignment">The alignment of the underlying buffer.</param>
    public AlignedNativeMemoryOwnerBufferWriter(int initialCapacity = DefaultInitialCapacity, bool clear = false, int alignment = AlignedNativeMemoryOwner<T>.DefaultAlignment)
    {
        Guard.IsGreaterThanOrEqualTo(initialCapacity, 0, nameof(initialCapacity));
        Guard.IsGreaterThan(alignment, 0, nameof(alignment));

        _owner = AlignedNativeMemoryOwner<T>.Allocate(initialCapacity, false, alignment);
        _resize = true;
        _clear = clear;
        _dispose = true;
    }

    /// <summary>
    /// Creates an instance of <see cref="AlignedNativeMemoryOwnerBufferWriter{T}"/> over the specified <see cref="AlignedNativeMemoryOwner{T}"/> instance.
    /// </summary>
    /// <param name="owner">The memory owner that this writer will write to.</param>
    /// <param name="index">The index at which to start writing.</param>
    /// <param name="resize">Whether to auto-resize the underlying buffer on overflow.</param>
    /// <param name="dispose">Whether to dispose the underlying buffer when this writer is disposed.</param>
    public AlignedNativeMemoryOwnerBufferWriter(AlignedNativeMemoryOwner<T> owner, int index = 0, bool resize = false, bool clear = false, bool dispose = false)
    {
        Guard.IsNotNull(owner, nameof(owner));
        Guard.IsGreaterThanOrEqualTo(index, 0, nameof(index));
        Guard.IsLessThan(index, owner.Length, nameof(index));

        _owner = owner;
        _index = index;
        _resize = resize;
        _clear = clear;
        _dispose = dispose;
    }

    private void AutoResize(int target)
    {
        // noop if no resizing needed
        if (target <= _owner.Length)
        {
            return;
        }

        // resize to the next power of two
        target = (int)BitOperations.RoundUpToPowerOf2((uint)target);

        // throw if not allowed to resize
        if (!_resize)
        {
            ThrowHelper.ThrowInvalidOperationException($"Cannot auto-resize buffer from {_owner.Length} to {target} because auto-resizing is not enabled");
        }

        // apply the resize operation
        _owner.Resize(target, _clear);
    }

    public void Advance(int count)
    {
        _index += count;

        AutoResize(_index);
    }

    public Memory<T> GetMemory(int sizeHint = 0)
    {
        Guard.IsGreaterThanOrEqualTo(sizeHint, 0, nameof(sizeHint));

        // ensure the writing buffer can hold at least one slot
        AutoResize(_index + Math.Max(sizeHint, 1));

        // return the available writing buffer
        return _owner.Memory[_index..];
    }

    public Span<T> GetSpan(int sizeHint = 0)
    {
        Guard.IsGreaterThanOrEqualTo(sizeHint, 0, nameof(sizeHint));

        // ensure the writing buffer can hold at least one slot
        AutoResize(_index + Math.Max(sizeHint, 1));

        // return the available writing buffer
        return _owner.GetSpan()[_index..];
    }

    /// <summary>
    /// Resizes the buffer down to the written portion, releasing the memory from the unwritten portion.
    /// Call this when you do not want to write to the buffer any further yet do want to hold on to the buffer for a long time.
    /// </summary>
    public void Trim()
    {
        _owner.Resize(_index, _clear);
    }

    public void Clear()
    {
        _index = 0;
    }

    public ReadOnlyMemory<T> WrittenMemory => _owner.Memory[.._index];

    public ReadOnlySpan<T> WrittenSpan => _owner.GetSpan()[.._index];

    public int WrittenCount => _index;

    public int Capacity => _owner.Length;

    public int FreeCapacity => _owner.Length - _index;

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        if (_dispose)
        {
            ((IDisposable)_owner).Dispose();
        }

        GC.SuppressFinalize(this);
        _disposed = true;
    }

    ~AlignedNativeMemoryOwnerBufferWriter()
    {
        Dispose();
    }
}