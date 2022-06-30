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

    /// <summary>
    /// Adapted from <see cref="BinaryWriter.Write7BitEncodedInt(int)"/>.
    /// </summary>
    public static void Write7BitEncodedInt<TBufferWriter>(ref this Writer<TBufferWriter> writer, int value)
        where TBufferWriter : IBufferWriter<byte>
    {
        uint uValue = (uint)value;

        while (uValue > 0x7Fu)
        {
            writer.WriteByte((byte)(uValue | ~0x7Fu));
            uValue >>= 7;
        }

        writer.WriteByte((byte)uValue);
    }

    /// <summary>
    /// Adapted from <see cref="BinaryWriter.Write7BitEncodedInt64(long)"/>.
    /// </summary>
    public static void Write7BitEncodedInt64<TBufferWriter>(ref this Writer<TBufferWriter> writer, long value)
        where TBufferWriter : IBufferWriter<byte>
    {
        ulong uValue = (ulong)value;

        while (uValue > 0x7Fu)
        {
            writer.WriteByte((byte)((uint)uValue | ~0x7Fu));
            uValue >>= 7;
        }

        writer.WriteByte((byte)uValue);
    }

    public static int Read7BitEncodedInt<TInput>(ref this Reader<TInput> reader)
    {
        // Unlike writing, we can't delegate to the 64-bit read on
        // 64-bit platforms. The reason for this is that we want to
        // stop consuming bytes if we encounter an integer overflow.

        uint result = 0;
        byte byteReadJustNow;

        // Read the integer 7 bits at a time. The high bit
        // of the byte when on means to continue reading more bytes.
        //
        // There are two failure cases: we've read more than 5 bytes,
        // or the fifth byte is about to cause integer overflow.
        // This means that we can read the first 4 bytes without
        // worrying about integer overflow.

        const int MaxBytesWithoutOverflow = 4;
        for (int shift = 0; shift < MaxBytesWithoutOverflow * 7; shift += 7)
        {
            // ReadByte handles end of stream cases for us.
            byteReadJustNow = reader.ReadByte();
            result |= (byteReadJustNow & 0x7Fu) << shift;

            if (byteReadJustNow <= 0x7Fu)
            {
                return (int)result; // early exit
            }
        }

        // Read the 5th byte. Since we already read 28 bits,
        // the value of this byte must fit within 4 bits (32 - 28),
        // and it must not have the high bit set.

        byteReadJustNow = reader.ReadByte();
        if (byteReadJustNow > 0b_1111u)
        {
            ThrowHelper.ThrowFormatException();
        }

        result |= (uint)byteReadJustNow << (MaxBytesWithoutOverflow * 7);
        return (int)result;
    }

    public static long Read7BitEncodedInt64<TInput>(ref this Reader<TInput> reader)
    {
        ulong result = 0;
        byte byteReadJustNow;

        // Read the integer 7 bits at a time. The high bit
        // of the byte when on means to continue reading more bytes.
        //
        // There are two failure cases: we've read more than 10 bytes,
        // or the tenth byte is about to cause integer overflow.
        // This means that we can read the first 9 bytes without
        // worrying about integer overflow.

        const int MaxBytesWithoutOverflow = 9;
        for (int shift = 0; shift < MaxBytesWithoutOverflow * 7; shift += 7)
        {
            // ReadByte handles end of stream cases for us.
            byteReadJustNow = reader.ReadByte();
            result |= (byteReadJustNow & 0x7Ful) << shift;

            if (byteReadJustNow <= 0x7Fu)
            {
                return (long)result; // early exit
            }
        }

        // Read the 10th byte. Since we already read 63 bits,
        // the value of this byte must fit within 1 bit (64 - 63),
        // and it must not have the high bit set.

        byteReadJustNow = reader.ReadByte();
        if (byteReadJustNow > 0b_1u)
        {
            ThrowHelper.ThrowFormatException();
        }

        result |= (ulong)byteReadJustNow << (MaxBytesWithoutOverflow * 7);
        return (long)result;
    }
}