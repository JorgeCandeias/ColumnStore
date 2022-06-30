namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// Base class for encoding implementations.
/// </summary>
internal abstract class Encoding<T>
{
    /// <summary>
    /// Encodes the specified values using the specified writer.
    /// </summary>
    public abstract void Encode<TBufferWriter>(ReadOnlySpan<T> source, TBufferWriter bufferWriter)
        where TBufferWriter : IBufferWriter<byte>;

    /// <summary>
    /// Decodes all underlying values in the specified sequence.
    /// </summary>
    public abstract IMemoryOwner<T> Decode(ReadOnlySpan<byte> source);

    /// <summary>
    /// Decodes all underlying ranges for the specified value in the specified sequence.
    /// </summary>
    public abstract IMemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, T value);

    /// <summary>
    /// Decodes all underlying ranges that fall between the specified window.
    /// </summary>
    public abstract IMemoryOwner<ValueRange<T>> Decode(ReadOnlySpan<byte> source, int start, int length);
}

/// <summary>
/// Defines a range in which a given value exists.
/// </summary>
internal record struct ValueRange<T>(T Value, int Start, int Length);

internal class Encoder<T>
{
    private readonly EncodingLookup _encodings;

    public Encoder(EncodingLookup encodings)
    {
        Guard.IsNotNull(encodings, nameof(encodings));

        _encodings = encodings;
    }

    /// <summary>
    /// Encodes the specified values using the specified writer.
    /// </summary>
    public void Encode(ReadOnlySequence<T> sequence, IBufferWriter<byte> writer, Compression compression)
    {
        Guard.IsNotNull(writer, nameof(writer));

        switch (compression)
        {
            case Compression.None: return;
            case Compression.Size: EncodeForSize(sequence, writer); return;
            case Compression.Speed: EncodeForSpeed(sequence, writer); return;
        }
    }

    private void EncodeForSize(ReadOnlySequence<T> sequence, IBufferWriter<byte> writer)
    {
    }

    private void EncodeForSpeed(ReadOnlySequence<T> sequence, IBufferWriter<byte> writer)
    {
    }
}

internal enum Compression
{
    None,
    Size,
    Speed
}