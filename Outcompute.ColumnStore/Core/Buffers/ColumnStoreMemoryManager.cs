using System.Runtime.InteropServices;

namespace Outcompute.ColumnStore.Core.Buffers;

internal class ColumnStoreMemoryManager : MemoryManager<byte>
{
    private readonly unsafe void* _memory;
    private readonly int _offset;
    private readonly int _length;

    public unsafe ColumnStoreMemoryManager(void* memory, int offset, int length)
    {
        _memory = memory;
        _offset = offset;
        _length = length;
    }

    public override Span<byte> GetSpan()
    {

    }

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        throw new NotImplementedException();
    }

    public override void Unpin()
    {
        throw new NotImplementedException();
    }

    private int _disposed = 0;

    protected override void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
        {
            return;
        }

        unsafe
        {
            NativeMemory.AlignedFree(_memory);
            GC.RemoveMemoryPressure(_length);
        }
    }

    private unsafe void* GetPointer(int index)
    {
        var position = _memory + _offset + index;
        return position.ToPointer();
    }
}