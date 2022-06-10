using Orleans.Serialization;
using Orleans.Serialization.Session;
using System.Collections;

namespace Outcompute.ColumnStore;

/// <summary>
/// A generic column segment that supports any type.
/// </summary>
internal class ColumnSegment<TValue> : IColumnSegment<TValue>
{
    private readonly byte[] _data;
    private readonly ColumnSegmentStats _stats;
    private readonly Serializer<ColumnSegmentHeader<TValue>> _headerSerializer;
    private readonly Serializer<ColumnSegmentRange> _rangeSerializer;
    private readonly SerializerSessionPool _sessions;

    public ColumnSegment(byte[] data, ColumnSegmentStats stats, Serializer<ColumnSegmentHeader<TValue>> headerSerializer, Serializer<ColumnSegmentRange> rangeSerializer, SerializerSessionPool sessions)
    {
        _data = data;
        _stats = stats;
        _headerSerializer = headerSerializer;
        _rangeSerializer = rangeSerializer;
        _sessions = sessions;
    }

    public IColumnSegmentStats GetStats() => _stats;

    /// <summary>
    /// Performs a full scan of the segment and yields every underlying source value as a regular collection would.
    /// This is a low performance fallback for cases where the user query cannot be evaluated in a more efficient way.
    /// </summary>
    public IEnumerator<TValue> GetEnumerator()
    {
        using var session = _sessions.GetSession();
        using var stream = new MemoryStream(_data);

        while (stream.Position < stream.Length)
        {
            var header = _headerSerializer.Deserialize(stream);
            for (var i = 0; i < header.RangeCount; i++)
            {
                var range = _rangeSerializer.Deserialize(stream);
                for (var j = range.Start; j <= range.End; j++)
                {
                    yield return header.Value;
                }
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}