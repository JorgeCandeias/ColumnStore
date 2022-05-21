namespace Outcompute.ColumnStore;

internal interface IDeltaRowGroupFactory<TRow>
{
    IRowGroup<TRow> Create(int id);
}