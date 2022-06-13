using Microsoft.IO;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Collections;

namespace Outcompute.ColumnStore;

/// <summary>
/// A generic column segment that supports any type.
/// </summary>
internal sealed class ColumnSegment<TValue> : IEnumerable<TValue>, IDisposable
{
    private readonly RecyclableMemoryStream _data;
    private readonly ColumnSegmentStats _stats;
    private readonly Serializer<ColumnSegmentHeader<TValue>> _headerSerializer;
    private readonly Serializer<ColumnSegmentRange> _rangeSerializer;
    private readonly SerializerSessionPool _sessions;

    public ColumnSegment(RecyclableMemoryStream data, ColumnSegmentStats stats, Serializer<ColumnSegmentHeader<TValue>> headerSerializer, Serializer<ColumnSegmentRange> rangeSerializer, SerializerSessionPool sessions)
    {
        _data = data;
        _stats = stats;
        _headerSerializer = headerSerializer;
        _rangeSerializer = rangeSerializer;
        _sessions = sessions;
    }

    public ColumnSegmentStats GetStats() => _stats;

    /// <summary>
    /// Performs a full scan of the segment and yields every underlying source value as a regular collection would.
    /// This is a low performance fallback for cases where the user query cannot be evaluated in a more efficient way.
    /// </summary>
    public IEnumerator<TValue> GetEnumerator()
    {
        using var session = _sessions.GetSession();

        var sequence = _data.GetReadOnlySequence();

        while (sequence.Length > 0)
        {
            var reader = Reader.Create(sequence, session);
            var header = _headerSerializer.Deserialize(ref reader);
            sequence.Slice(reader.Position);

            for (var i = 0; i < header.RangeCount; i++)
            {
                reader = Reader.Create(sequence, session);
                var range = _rangeSerializer.Deserialize(ref reader);
                sequence = sequence.Slice(reader.Position);

                for (var j = range.Start; j <= range.End; j++)
                {
                    yield return header.Value;
                }
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    #region Disposable

    private void DisposeCore()
    {
        _data.Dispose();
    }

    public void Dispose()
    {
        DisposeCore();
        GC.SuppressFinalize(this);
    }

    ~ColumnSegment()
    {
        DisposeCore();
    }

    #endregion Disposable
}