namespace Outcompute.ColumnStore;

public interface IDeltaStore<TRow> : IReadOnlyCollection<TRow>
{
    void Add(TRow row);

    int AddRange(IEnumerable<TRow> rows);

    void Close();

    bool TryTakeClosed(out IRowGroup<TRow> group);

    InnerStoreStats Stats { get; }
}