namespace Outcompute.ColumnStore;

internal interface IDeltaRowGroupFactory<TRow>
{
    IDeltaRowGroup<TRow> Create(int id);
}