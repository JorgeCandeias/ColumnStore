using Microsoft.Extensions.DependencyInjection;
using Microsoft.IO;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Buffers;

namespace Outcompute.ColumnStore.Segments.Specialized;

/// <summary>
/// Specialized column segment builder for stack-based primitives.
/// </summary>
internal sealed class ValuePrimitiveColumnSegmentBuilder<T> : ColumnSegmentBuilder<T>
    where T : struct
{
    private readonly Serializer<T> _serializer;

    public ValuePrimitiveColumnSegmentBuilder(IComparer<T> comparer, IServiceProvider provider, SerializerSessionPool sessions, Serializer<T> serializer)
        : base(comparer, provider, sessions)
    {
        Guard.IsNotNull(serializer, nameof(serializer));

        _serializer = serializer;
    }

    protected override ColumnSegment<T> OnCreate(RecyclableMemoryStream stream, ColumnSegmentStats stats)
    {
        return ActivatorUtilities.CreateInstance<ValuePrimitiveColumnSegment<T>>(ServiceProvider, stream, stats, Comparer);
    }

    protected override void OnSerializeValue(T value, ref Writer<IBufferWriter<byte>> writer)
    {
        _serializer.Serialize(value, ref writer);
    }
}