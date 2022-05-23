using Microsoft.IO;

namespace Outcompute.ColumnStore;

internal static class MemoryStreamManager
{
    public static RecyclableMemoryStreamManager Default { get; } = new RecyclableMemoryStreamManager();
}