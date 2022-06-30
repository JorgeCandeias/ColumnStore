namespace Outcompute.ColumnStore.Encodings;

internal class Int32SequentialEncoding : SequentialEncoding<int>
{
    public Int32SequentialEncoding(SerializerSessionPool sessions) : base(sessions)
    {
    }

    protected override WellKnownEncodings EncodingId => WellKnownEncodings.Sequential;

    protected override void Serialize<TBufferWriter>(ref Writer<TBufferWriter> writer, int value)
    {
        //writer.Write7BitEncodedInt(value);
        writer.WriteVarUInt32((uint)value);
    }

    protected override int Deserialize<TInput>(ref Reader<TInput> reader)
    {
        //return reader.Read7BitEncodedInt();
        return (int)reader.ReadVarUInt32();
    }
}