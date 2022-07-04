namespace Outcompute.ColumnStore.Encodings;

/// <summary>
/// A sequential encoding specialized
/// </summary>
internal class Int32SequentialEncoding : SequentialEncoding<int>
{
    public Int32SequentialEncoding(SerializerSessionPool sessions) : base(sessions)
    {
    }

    protected override WellKnownEncodings EncodingId => WellKnownEncodings.Sequential;

    protected override void Serialize<TBufferWriter>(ref Writer<TBufferWriter> writer, int value)
    {
        writer.WriteVarUInt32((uint)value);
    }

    protected override int Deserialize<TInput>(ref Reader<TInput> reader)
    {
        return (int)reader.ReadVarUInt32();
    }
}