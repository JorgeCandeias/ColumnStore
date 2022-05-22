namespace Outcompute.ColumnStore;

public interface ISolidRowGroupFactory<TRow>
{
    ISolidRowGroup<TRow> Create(IRowGroup<TRow> source);
}