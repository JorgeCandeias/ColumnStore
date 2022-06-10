namespace Outcompute.ColumnStore;

public interface IColumnSegmentStats
{
    string Name { get; }

    int DistinctValueCount { get; }

    int DefaultValueCount { get; }
}