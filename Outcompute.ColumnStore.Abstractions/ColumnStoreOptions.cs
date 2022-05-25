using System.ComponentModel.DataAnnotations;

namespace Outcompute.ColumnStore;

public class ColumnStoreOptions
{
    /// <summary>
    /// The size of an uncompressed rowgroup in the delta store which will trigger the compression process.
    /// Defaults to one million rows.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int RowGroupSizeThreshold { get; set; } = 1000000;
}