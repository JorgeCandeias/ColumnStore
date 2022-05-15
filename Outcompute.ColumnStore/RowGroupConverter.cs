namespace Outcompute.ColumnStore;

internal class RowGroupConverter<TRow>
{
    public CompressedRowGroup<TRow> Convert(UncompressedRowGroup<TRow> group)
    {
        Guard.IsNotNull(group, nameof(group));

        // todo: perform a heuristic to order the data by column cardinality

        var result = CompressedRowGroupFactory.Create<TRow>();

        foreach (var row in group)
        {
            result.Add(row);
        }

        return result;
    }
}