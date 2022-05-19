namespace Outcompute.ColumnStore;

internal interface IDeltaRowGroupFactory<out TRow>
{
    IRowGroup<TRow> Create(int id);
}