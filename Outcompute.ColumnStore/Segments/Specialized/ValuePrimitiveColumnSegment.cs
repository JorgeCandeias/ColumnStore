using Microsoft.IO;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;

namespace Outcompute.ColumnStore.Segments.Specialized;

/// <summary>
/// Specialized column segment for stack-based primitives.
/// </summary>
internal sealed class ValuePrimitiveColumnSegment<T> : ColumnSegment<T>
{
    public ValuePrimitiveColumnSegment(RecyclableMemoryStream data, ColumnSegmentStats stats, IComparer<T> comparer, Serializer<T> valueSerializer, SerializerSessionPool sessions)
        : base(data, stats, comparer, valueSerializer, sessions)
    {
    }

    private readonly record struct SortedRangeResult(int Start, int End, T Value) : IComparable<SortedRangeResult>
    {
        public int CompareTo(ValuePrimitiveColumnSegment<T>.SortedRangeResult other)
        {
            return Start.CompareTo(other.Start);
        }
    }

    protected override IEnumerator<T> OnEnumerate()
    {
        if (Count == 0)
        {
            yield break;
        }

        using var session = SessionPool.GetSession();
        var sequence = Data.GetReadOnlySequence();
        var reader = Reader.Create(sequence, session);

        // read the total group count
        var totalGroups = reader.ReadVarUInt32();

        // read the total range count
        _ = reader.ReadVarUInt32();

        var result = new SortedSet<SortedRangeResult>();

        // read each group
        for (var g = 0; g < totalGroups; g++)
        {
            // read the value
            var value = ValueSerializer.Deserialize(ref reader);

            // read the range count
            var ranges = reader.ReadVarUInt32();

            // read each range
            for (var r = 0; r < ranges; ++r)
            {
                // read the start index
                var start = (int)reader.ReadVarUInt32();

                // read the end index
                var end = (int)reader.ReadVarUInt32();

                // keep the range
                result.Add(new SortedRangeResult(start, end, value));
            }
        }


        // todo: separate encoding from column segment

        // yield the ranges now that we are done with the ref reader


        var enumerator = QueryByRange(0, Count - 1);
        var sorted = new SortedDictionary<int, (int End, T Value)>();

        while (enumerator.MoveNext())
        {
            var range = enumerator.Current;

            sorted[range.Start] = (range.End, range.Value);
        }

        foreach (var item in sorted)
        {
            for (var i = item.Key; i <= item.Value.End; i++)
            {
                yield return item.Value.Value;
            }
        }
    }
}