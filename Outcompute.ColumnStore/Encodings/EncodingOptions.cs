namespace Outcompute.ColumnStore.Encodings;

internal class EncodingOptions
{
    // todo: remove this when the new PooledArrayBufferWriter is available
    /// <summary>
    /// Buffer size used to serialize individual values.
    /// </summary>
    public int ValueBufferSize { get; set; } = 1024 * 1024;
}