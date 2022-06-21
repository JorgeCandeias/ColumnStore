using CommunityToolkit.HighPerformance.Buffers;
using Orleans;
using System.Buffers;

namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// Base class for encoding implementations.
/// </summary>
internal abstract class Encoding<T>
{
    /// <summary>
    /// Encodes the specified values using the specified writer.
    /// </summary>
    public abstract void Encode(IBufferWriter<byte> writer, ReadOnlySequence<T> sequence);

    /// <summary>
    /// Decodes all underlying values in the specified sequence.
    /// </summary>
    public abstract MemoryOwner<T> Decode(ReadOnlySequence<byte> sequence);

    /// <summary>
    /// Decodes all underlying ranges for the specified value in the specified sequence
    /// </summary>
    public abstract MemoryOwner<ValueRange<T>> Decode(ReadOnlySequence<byte> sequence, T value);

    /// <summary>
    /// Decodes all underlying ranges that fall between the specified window.
    /// </summary>
    public abstract MemoryOwner<ValueRange<T>> Decode(ReadOnlySequence<byte> sequence, int start, int length);
}

/// <summary>
/// Defines a range in which a given value exists.
/// </summary>
internal record struct ValueRange<T>(T Value, int Start, int Length);

[GenerateSerializer]
internal enum Encodings
{
    Default = 0
}