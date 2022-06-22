using CommunityToolkit.HighPerformance.Buffers;
using Microsoft.Extensions.DependencyInjection;
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
    public abstract void Encode(ReadOnlySequence<T> sequence, IBufferWriter<byte> writer);

    /// <summary>
    /// Decodes all underlying values in the specified sequence.
    /// </summary>
    public abstract MemoryOwner<T> Decode(ReadOnlySequence<byte> sequence);

    /// <summary>
    /// Decodes all underlying ranges for the specified value in the specified sequence.
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

/// <summary>
/// Defines a range in which a given value exists.
/// </summary>
internal record struct ValueRange(int Start, int Length);

[GenerateSerializer]
internal enum WellKnownEncodings
{
    Default = 0,
    Dictionary = 1
}

internal class EncodingLookup
{
    private readonly IServiceProvider _provider;
    private readonly Dictionary<int, Type> _lookup;
    private readonly Dictionary<(Type, Type), Type> _constructed = new();

    public EncodingLookup(IServiceProvider provider, IEnumerable<EncodingRegistryEntry> entries)
    {
        Guard.IsNotNull(provider, nameof(provider));
        Guard.IsNotNull(entries, nameof(entries));

        _provider = provider;
        _lookup = entries
            .GroupBy(x => x.Id)
            .Select(x => x.Last())
            .ToDictionary(x => x.Id, x => x.Type);
    }

    public Encoding<T> Get<T>(int id)
    {
        if (!_lookup.TryGetValue(id, out var type))
        {
            ThrowHelper.ThrowInvalidOperationException($"No encoding registered for id '{id}'");
        }

        if (type.IsGenericTypeDefinition)
        {
            if (!_constructed.TryGetValue((type, typeof(T)), out var constructed))
            {
                _constructed[(type, typeof(T))] = constructed = type.MakeGenericType(typeof(T));
            }

            type = constructed;
        }

        var encoding = _provider.GetService(type);

        if (encoding is null)
        {
            ThrowHelper.ThrowInvalidOperationException($"Could not resolve encoding of type '{type.ToTypeString()}' for id '{id}'");
        }

        return (Encoding<T>)encoding;
    }
}

internal record class EncodingRegistryEntry(int Id, Type Type);

public static class EncodingRegistryServiceCollectionExtensions
{
    public static IServiceCollection AddEncoding(this IServiceCollection services, int id, Type type)
    {
        Guard.IsNotNull(services, nameof(services));

        if (type.IsGenericTypeDefinition)
        {
            if (!type.IsSubclassOf(typeof(Encoding<>)))
            {
                Throw();
            }
        }
        else if (type.IsConstructedGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            if (!definition.IsSubclassOf(typeof(Encoding<>)))
            {
                Throw();
            }
        }
        else
        {
            Throw();
        }

        return services
            .AddSingleton(type)
            .AddSingleton(typeof(EncodingRegistryEntry), new EncodingRegistryEntry(id, type));

        void Throw() => ThrowHelper.ThrowInvalidOperationException($"Type {type.ToTypeString()} must implement {typeof(Encoding<>).ToTypeString()}");
    }

    public static IServiceCollection AddEncoding<TEncoding>(this IServiceCollection services, byte id)
    {
        Guard.IsNotNull(services, nameof(services));

        return services
            .AddEncoding(id, typeof(TEncoding));
    }
}

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