namespace Outcompute.ColumnStore;

public interface IDeltaStoreFactory<TRow>
{
    IDeltaStore<TRow> Create(int rowGroupCapacity);
}