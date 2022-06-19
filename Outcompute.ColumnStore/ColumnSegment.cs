using Microsoft.IO;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Buffers;
using System.Collections;

namespace Outcompute.ColumnStore;

/// <summary>
/// A generic column segment that supports any type.
/// </summary>
internal sealed class ColumnSegment<TValue> : IReadOnlyCollection<TValue>, IDisposable
{
    private readonly RecyclableMemoryStream _data;
    private readonly ColumnSegmentStats _stats;
    private readonly IComparer<TValue> _comparer;
    private readonly Serializer<TValue> _valueSerializer;
    private readonly SerializerSessionPool _sessions;

    public ColumnSegment(RecyclableMemoryStream data, ColumnSegmentStats stats, IComparer<TValue> comparer, Serializer<TValue> valueSerializer, SerializerSessionPool sessions)
    {
        Guard.IsNotNull(data, nameof(data));
        Guard.IsNotNull(stats, nameof(stats));
        Guard.IsNotNull(comparer, nameof(comparer));
        Guard.IsNotNull(valueSerializer, nameof(valueSerializer));
        Guard.IsNotNull(sessions, nameof(sessions));

        _data = data;
        _stats = stats;
        _comparer = comparer;
        _valueSerializer = valueSerializer;
        _sessions = sessions;
    }

    public ColumnSegmentStats Stats => _stats;

    public int Count => _stats.RowCount;

    public IEnumerator<RangeQueryResult> QueryByValue(TValue value)
    {
        using var session = _sessions.GetSession();
        var sequence = _data.GetReadOnlySequence();
        var reader = Reader.Create(sequence, session);

        // read the total group count
        var totalGroups = reader.ReadVarUInt32();

        // read the total range count
        var totalRanges = reader.ReadVarUInt32();
        var buffer = ArrayPool<RangeQueryResult>.Shared.Rent((int)totalRanges);
        var count = 0;

        // read all groups to find the correct one
        for (var g = 0; g < totalGroups; g++)
        {
            var candidate = _valueSerializer.Deserialize(ref reader);
            var ranges = reader.ReadVarUInt32();

            // check if the group is relevant
            if (_comparer.Compare(value, candidate) is 0)
            {
                // consume groups
                for (var r = 0; r < ranges; r++)
                {
                    var start = (int)reader.ReadVarUInt32();
                    var end = (int)reader.ReadVarUInt32();

                    buffer[count++] = new RangeQueryResult(start, end);
                }
            }
            else
            {
                // ignore all ranges
                for (var r = 0; r < ranges; r++)
                {
                    _ = reader.ReadVarUInt32();
                    _ = reader.ReadVarUInt32();
                }
            }

            if (candidate is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        for (var i = 0; i < count; i++)
        {
            yield return buffer[i];
        }

        ArrayPool<RangeQueryResult>.Shared.Return(buffer);
    }

    // todo: optimize this via duck typing with a disposable struct enumerator
    public IEnumerator<RangeQueryResult<TValue>> QueryByRange(int start, int end)
    {
        Guard.IsGreaterThanOrEqualTo(start, 0, nameof(start));
        Guard.IsLessThan(start, Count, nameof(start));
        Guard.IsGreaterThanOrEqualTo(end, start, nameof(end));
        Guard.IsLessThan(end, Count, nameof(end));

        using var session = _sessions.GetSession();
        var sequence = _data.GetReadOnlySequence();
        var reader = Reader.Create(sequence, session);

        // read the total group count
        var totalGroups = reader.ReadVarUInt32();

        // read the total range count
        var totalRanges = reader.ReadVarUInt32();

        // read all ranges into a buffer as yield is incompatible with the ref reader
        var resultBuffer = ArrayPool<RangeQueryResult<TValue>>.Shared.Rent((int)totalRanges);
        var resultCount = 0;

        // read each group
        for (var g = 0; g < totalGroups; g++)
        {
            // read the group value
            // todo: this is inneficient for non-primitives
            // todo: optimize this using an optional lookup serialization format
            var value = _valueSerializer.Deserialize(ref reader);

            // read the range count
            var ranges = reader.ReadVarUInt32();

            // read each range
            for (var r = 0; r < ranges; ++r)
            {
                // read the start index
                var s = (int)reader.ReadVarUInt32();

                // read the end index
                var e = (int)reader.ReadVarUInt32();

                // adjust indexes to the query
                s = Math.Max(s, start);
                e = Math.Min(e, end);

                // check if the range still applies
                if (s <= e)
                {
                    resultBuffer[resultCount++] = new RangeQueryResult<TValue>(s, e, value);
                }
            }
        }

        // yield the ranges now that we are done with the ref reader
        for (var i = 0; i < resultCount; i++)
        {
            yield return resultBuffer[i];
        }

        // todo: this may not run if the enumeration is cancelled midway
        // todo: this needs refactoring into an isolated enumerator
        ArrayPool<RangeQueryResult<TValue>>.Shared.Return(resultBuffer, true);
    }

    /// <summary>
    /// Performs a full scan of the segment and yields every underlying source value as a regular collection would.
    /// This is a low performance fallback for cases where the user query cannot be evaluated in a more efficient way.
    /// </summary>
    public IEnumerator<TValue> GetEnumerator()
    {
        if (Count == 0)
        {
            yield break;
        }

        // todo: for now just rely on the range query but this needs optimization
        var enumerator = QueryByRange(0, Count - 1);
        var sorted = new SortedDictionary<int, (int End, TValue Value)>();

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