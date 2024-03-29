﻿using Microsoft.IO;
using Orleans;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Session;
using System.Buffers;
using System.Collections;

namespace Outcompute.ColumnStore;

/// <summary>
/// Holds an uncompressed group of rows.
/// </summary>
[GenerateSerializer]
[UseActivator]
public abstract class DeltaRowGroup<TRow> : IDeltaRowGroup<TRow>, IDisposable
{
    private readonly Serializer<TRow> _serializer;
    private readonly SerializerSessionPool _sessions;

    protected DeltaRowGroup(int id, int capacity, Serializer<TRow> serializer, SerializerSessionPool sessions)
    {
        Guard.IsGreaterThanOrEqualTo(id, 0, nameof(id));
        Guard.IsGreaterThanOrEqualTo(capacity, 0, nameof(capacity));
        Guard.IsNotNull(serializer, nameof(serializer));
        Guard.IsNotNull(sessions, nameof(sessions));

        Id = id;
        Capacity = capacity;
        _serializer = serializer;
        _sessions = sessions;
    }

    [Id(1)]
    public int Id { get; }

    [Id(2)]
    public int Capacity { get; }

    [Id(3)]
    public RowGroupState State { get; private set; } = RowGroupState.Open;

    [Id(4)]
    private readonly RecyclableMemoryStream _data = ColumnStoreMemoryStreamManager.GetStream();

    [Id(5)]
    private RowGroupStats? _stats;

    [Id(6)]
    public int Count { get; private set; }

    /// <summary>
    /// Version used to invalidate enumerators.
    /// </summary>
    [Id(7)]
    public int Version { get; private set; }

    public RowGroupStats Stats => _stats ??= BuildStats();

    /// <summary>
    /// Gets the underlying serialized data.
    /// </summary>
    public ReadOnlySequence<byte> GetReadOnlySequence() => _data.GetReadOnlySequence();

    private void Invalidate()
    {
        _stats = null;
        unchecked
        {
            Version++;
        }
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
        if (Count >= Capacity)
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
    public int AddRange(IEnumerable<TRow> rows)
    {
        EnsureOpen();

        var added = 0;
        foreach (var row in rows)
        {
            Pack(row);
            OnAdded(row);
            added++;
        }

        if (added > 0)
        {
            TryClose();
            Invalidate();
        }

        return added;
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
        var version = Version;

        using var session = _sessions.GetSession();
        var sequence = _data.GetReadOnlySequence();

        while (sequence.Length > 0)
        {
            if (version != Version)
            {
                ThrowHelper.ThrowInvalidOperationException($"This {GetType().Name} has changed since the enumeration started");
            }

            var reader = Reader.Create(sequence, session);
            var result = _serializer.Deserialize(ref reader);
            sequence = sequence.Slice(reader.Position);

            yield return result;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    #region Disposable

    protected virtual void Dispose(bool disposing)
    {
        _data.Dispose();
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~DeltaRowGroup()
    {
        Dispose(false);
    }

    #endregion Disposable
}