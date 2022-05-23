using Microsoft.Extensions.Options;
using Microsoft.IO;
using Orleans;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Collections;

namespace Outcompute.ColumnStore;

/// <summary>
/// Holds an uncompressed group of rows.
/// </summary>
[GenerateSerializer]
public abstract class DeltaRowGroup<TRow> : IDeltaRowGroup<TRow>
{
    private readonly ColumnStoreOptions _options;
    private readonly Serializer<TRow> _serializer;
    private readonly SerializerSessionPool _sessions;

    protected DeltaRowGroup(int id, IOptions<ColumnStoreOptions> options, Serializer<TRow> serializer, SerializerSessionPool sessions)
    {
        Guard.IsGreaterThanOrEqualTo(id, 0, nameof(id));
        Guard.IsNotNull(options, nameof(options));
        Guard.IsNotNull(serializer, nameof(serializer));

        _options = options.Value;
        _serializer = serializer;
        _sessions = sessions;

        Id = id;
    }

    [Id(1)]
    public int Id { get; }

    [Id(2)]
    public RowGroupState State { get; private set; } = RowGroupState.Open;

    [Id(3)]
    private readonly RecyclableMemoryStream _data = (RecyclableMemoryStream)MemoryStreamManager.Default.GetStream();

    /*
    [Id(3)]
    private ReadOnlySequence<byte> DataBytes
    {
        get
        {
            return _data.GetReadOnlySequence();
        }
        set
        {
            Guard.IsLessThanOrEqualTo(value.Length, int.MaxValue, nameof(value));

            _data.SetLength(value.Length);
            _data.Position = 0;

            var length = value.Length;
            var copied = 0;

            while (copied < length)
            {
                var buffer = _data.GetSpan();
                var take = (int)Math.Min(buffer.Length, length - copied);
                value.CopyTo(buffer);
                _data.Advance(take);

                value = value.Slice(take);
                copied += take;
            }
        }
    }
    */

    [Id(4)]
    private RowGroupStats? _stats;

    public IRowGroupStats Stats
    {
        get
        {
            if (_stats is null)
            {
                _stats = BuildStats();
            }

            return _stats;
        }
    }

    [Id(5)]
    public int Count { get; private set; }

    private void Invalidate()
    {
        _stats = null;
    }

    /// <summary>
    /// Updates row group and column statistics.
    /// </summary>
    private RowGroupStats BuildStats()
    {
        var builder = RowGroupStats.CreateBuilder();

        builder.Id = Id;
        builder.RowCount = Count;

        OnBuildStats(builder);

        return builder.ToImmutable();
    }

    /// <summary>
    /// Implemented by generated classes to update column statistics.
    /// </summary>
    protected abstract void OnBuildStats(RowGroupStats.Builder builder);

    /// <summary>
    /// Verifies that this row group is open and throws if it is not.
    /// </summary>
    private void EnsureOpen()
    {
        if (State != RowGroupState.Open)
        {
            ThrowHelper.ThrowInvalidOperationException($"This {nameof(DeltaRowGroup<TRow>)} is not open");
        }
    }

    /// <summary>
    /// Closes the row group if the row count has reached the threshold.
    /// </summary>
    private void TryClose()
    {
        if (Count >= _options.RowGroupSizeThreshold)
        {
            State = RowGroupState.Closed;
        }
    }

    /// <summary>
    /// Adds the specified row to this row group.
    /// Will attempt to close the row group after consuming the row.
    /// </summary>
    public void Add(TRow row)
    {
        EnsureOpen();

        Pack(row);

        OnAdded(row);

        TryClose();

        Invalidate();
    }

    private void Pack(TRow row)
    {
        _serializer.Serialize(row, _data);

        Count++;
    }

    /// <summary>
    /// Used by generated classes to update the live statistic helpers upon adding a row.
    /// </summary>
    protected abstract void OnAdded(TRow row);

    /// <summary>
    /// Adds the specified batch to this row group.
    /// Will only attempt to close the row group after the entire batch is consumed.
    /// </summary>
    public void AddRange(IEnumerable<TRow> rows)
    {
        EnsureOpen();

        foreach (var row in rows)
        {
            Pack(row);

            OnAdded(row);
        }

        TryClose();

        Invalidate();
    }

    /// <summary>
    /// Marks the row group as closed so no additional rows are accepted.
    /// </summary>
    public void Close()
    {
        State = RowGroupState.Closed;
    }

    public IEnumerator<TRow> GetEnumerator()
    {
        using var session = _sessions.GetSession();
        var sequence = _data.GetReadOnlySequence();

        var position = 0;
        var length = sequence.Length;

        while (position < length)
        {
            // create a new reader per iteration as ref structs cant be used after yielding
            var reader = Reader.Create(sequence, session);
            reader.Skip(position);

            yield return _serializer.Deserialize(ref reader);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}