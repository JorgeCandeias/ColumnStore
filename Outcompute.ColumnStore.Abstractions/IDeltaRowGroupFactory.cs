namespace Outcompute.ColumnStore;

public interface IDeltaRowGroupFactory<TRow>
{
    IDeltaRowGroup<TRow> Create(int id, int capacity);
}