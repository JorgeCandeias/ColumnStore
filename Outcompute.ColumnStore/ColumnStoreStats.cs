namespace Outcompute.ColumnStore;

public record ColumnStoreStats
{
    public ColumnStoreStats(int count, int deltaRowCount, IReadOnlyList<RowGroupStats> rowGroupStats)
    {
        RowCount = count;
        DeltaRowCount = deltaRowCount;
        RowGroupStats = rowGroupStats;
    }

    /// <summary>
    /// Total number of rows represented by the columnstore.
    /// </summary>
    public int RowCount { get; }

    /// <summary>
    /// Total number of rows in the delta store.
    /// </summary>
    public int DeltaRowCount { get; }

    /// <summary>
    /// Detailed stats for compressed row groups.
    /// </summary>
    public IReadOnlyList<RowGroupStats> RowGroupStats { get; }
}

public record RowGroupStats
{
    public RowGroupStats(int rowCount, IReadOnlyList<ColumnSegmentStats> columnSegmentStats)
    {
        RowCount = rowCount;
        ColumnSegmentStats = columnSegmentStats;
    }

    /// <summary>
    /// The total number of rows represented by this row group.
    /// </summary>
    public int RowCount { get; }

    /// <summary>
    /// Detailed stats for each column segment.
    /// </summary>
    public IReadOnlyList<ColumnSegmentStats> ColumnSegmentStats { get; }
}

public record ColumnSegmentStats
{
    public ColumnSegmentStats(string propertyName, int rowCount, int rangeCount)
    {
        PropertyName = propertyName;
        RowCount = rowCount;
        RangeCount = rangeCount;
    }

    /// <summary>
    /// The name of the property that this column segment holds data for.
    /// </summary>
    public string PropertyName { get; }

    /// <summary>
    /// The total number of rows represented by this column segment.
    /// </summary>
    public int RowCount { get; }

    /// <summary>
    /// The total number of ranges held by this column segment.
    /// </summary>
    public int RangeCount { get; }
}