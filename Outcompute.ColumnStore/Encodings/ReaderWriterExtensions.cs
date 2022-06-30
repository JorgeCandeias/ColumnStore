using Orleans.Serialization.Buffers;

namespace Outcompute.ColumnStore.Encodings;

internal static class ReaderWriterExtensions
{
    #region EncodingId

    public static void WriteEncodingId<TBufferWriter>(ref this Writer<TBufferWriter> writer, WellKnownEncodings id)
        where TBufferWriter : IBufferWriter<byte>
    {
        writer.WriteVarUInt32((uint)id);
    }

    public static void VerifyEncodingId<TInput>(ref this Reader<TInput> reader, WellKnownEncodings id)
    {
        VerifyEncodingId(ref reader, (int)id);
    }

    public static void VerifyEncodingId<TInput>(ref Reader<TInput> reader, int id)
    {
        var value = (int)reader.ReadVarUInt32();
        if (value != id)
        {
            ThrowHelper.ThrowInvalidOperationException($"Payload does not start with the encoding id of '{id}'");
        }
    }

    #endregion EncodingId

    #region Hash

    public static void WriteHash<TBufferWriter>(ref this Writer<TBufferWriter> writer, ReadOnlySpan<byte> data)
        where TBufferWriter : IBufferWriter<byte>
    {
        var hash = JenkinsHash.ComputeHash(data);
        writer.WriteVarUInt32(hash);
    }

    public static uint ReadHash<TInput>(ref this Reader<TInput> reader) => reader.ReadVarUInt32();

    public static void SkipHash<TInput>(ref this Reader<TInput> reader) => _ = reader.ReadVarUInt32();

    #endregion Hash

    #region Count

    public static void WriteCount<TBufferWriter>(ref this Writer<TBufferWriter> writer, int count)
        where TBufferWriter : IBufferWriter<byte>
    {
        writer.WriteVarUInt32((uint)count);
    }

    public static int ReadCount<TInput>(ref this Reader<TInput> reader) => (int)reader.ReadVarUInt32();

    public static void SkipCount<TInput>(ref this Reader<TInput> reader) => _ = reader.ReadVarUInt32();

    #endregion Count
}