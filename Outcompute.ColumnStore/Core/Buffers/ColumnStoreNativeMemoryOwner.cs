namespace Outcompute.ColumnStore.Core.Buffers;

internal sealed class ColumnStoreNativeMemoryOwner : IMemoryOwner<byte>
{
    private readonly void* _memory;

    private ColumnStoreNativeMemoryOwner(void* memory)
    {
        MemoryManager
    }

    public Memory<byte> Memory => throw new NotImplementedException();

    public void Dispose()
    {
        throw new NotImplementedException();
    }

    public static ColumnStoreNativeMemoryOwner Allocate(int length)
    {
        void* memory = NativeMemory.AlignedAlloc(100, 64);
        var span = new Span<int>(memory, 10);
        span.Fill(0);
        return this;
    }
}