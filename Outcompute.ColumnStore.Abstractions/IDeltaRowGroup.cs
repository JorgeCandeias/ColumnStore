using Orleans;

namespace Outcompute.ColumnStore;

public interface IDeltaRowGroup<TRow> : IRowGroup<TRow>
{
    int Capacity { get; }

    ReadOnlyMemory<byte> DataBytes { get; }

    void Add(TRow row);

    void AddRange(IEnumerable<TRow> rows);

    void Close();
}