using Microsoft.IO;
using Orleans;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.WireProtocol;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Outcompute.ColumnStore.Serialization;

[RegisterSerializer]
internal sealed class RecyclableMemoryStreamCodec : TypedCodecBase<RecyclableMemoryStream, RecyclableMemoryStreamCodec>, IFieldCodec<RecyclableMemoryStream>
{
    private static readonly Type CodecFieldType = typeof(RecyclableMemoryStream);

    public void WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, RecyclableMemoryStream value) where TBufferWriter : IBufferWriter<byte>
    {
        if (ReferenceCodec.TryWriteReferenceField(ref writer, fieldIdDelta, expectedType, value))
        {
            return;
        }

        writer.WriteFieldHeader(fieldIdDelta, expectedType, CodecFieldType, WireType.LengthPrefixed);
        writer.WriteVarUInt64((ulong)value.Length);

        foreach (var memory in value.GetReadOnlySequence())
        {
            writer.Write(memory.Span);
        }
    }

    public RecyclableMemoryStream ReadValue<TInput>(ref Reader<TInput> reader, Field field)
    {
        if (field.WireType == WireType.Reference)
        {
            return ReferenceCodec.ReadReference<RecyclableMemoryStream, TInput>(ref reader, field);
        }

        if (field.WireType != WireType.LengthPrefixed)
        {
            ThrowUnsupportedWireTypeException(field);
        }

        var length = reader.ReadVarUInt64();

        var result = ColumnStoreMemoryStreamManager.GetStream();
        ulong required = length;
        while (required > 0)
        {
            var buffer = result.GetSpan();
            if ((ulong)buffer.Length > required)
            {
                buffer = buffer[..(int)required];
            }
            reader.ReadBytes(buffer);
            result.Advance(buffer.Length);

            required -= (ulong)buffer.Length;
        }

        ReferenceCodec.RecordObject(reader.Session, result);

        return result;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowUnsupportedWireTypeException(Field field) => throw new UnsupportedWireTypeException(
        $"Only a {nameof(WireType)} value of {WireType.LengthPrefixed} is supported for {nameof(RecyclableMemoryStream)} fields. {field}");
}

[RegisterCopier]
internal sealed class RecyclableMemoryStreamCopier : IDeepCopier<RecyclableMemoryStream>
{
    public RecyclableMemoryStream DeepCopy(RecyclableMemoryStream input, CopyContext context)
    {
        if (context.TryGetCopy<RecyclableMemoryStream>(input, out var result))
        {
            return result;
        }

        result = ColumnStoreMemoryStreamManager.GetStream();
        foreach (var memory in input.GetReadOnlySequence())
        {
            var buffer = result.GetMemory(memory.Length);
            memory.CopyTo(buffer);
            result.Advance(memory.Length);
        }

        context.RecordCopy(input, result);
        return result;
    }
}