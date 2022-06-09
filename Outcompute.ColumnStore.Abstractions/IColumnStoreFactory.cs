namespace Outcompute.ColumnStore;

public interface IColumnStoreFactory<TRow>
{
    IColumnStore<TRow> Create();

    IColumnStore<TRow> Create(ColumnStoreOptions options);
}