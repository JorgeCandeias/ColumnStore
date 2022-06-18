using Microsoft.IO;

namespace Outcompute.ColumnStore;

// todo: refactor this into a service
internal static class ColumnStoreMemoryStreamManager
{
    private static readonly RecyclableMemoryStreamManager _manager = new();

    public static RecyclableMemoryStream GetStream() => (RecyclableMemoryStream)_manager.GetStream();
}