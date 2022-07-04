namespace Outcompute.ColumnStore.Encodings;

internal class Int16SequentialEncoding : SequentialEncoding<short>
{
    public Int16SequentialEncoding(SerializerSessionPool sessions) : base(sessions)
    {
    }

    protected override WellKnownEncodings EncodingId => WellKnownEncodings.Sequential;

    protected override void Serialize<TBufferWriter>(ref Writer<TBufferWriter> writer, short value)
    {
        writer.WriteVarUInt32((uint)value);
    }

    protected override short Deserialize<TInput>(ref Reader<TInput> reader)
    {
        return (short)reader.ReadVarUInt32();
    }
}