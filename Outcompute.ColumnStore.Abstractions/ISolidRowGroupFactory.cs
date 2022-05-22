namespace Outcompute.ColumnStore;

internal interface ISolidRowGroupFactory<TRow>
{
    ISolidRowGroup<TRow> Create(int id, IEnumerable<TRow> rows);
}