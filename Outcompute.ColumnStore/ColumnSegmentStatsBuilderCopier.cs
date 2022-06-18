using Orleans;
using Orleans.Serialization.Cloning;

namespace Outcompute.ColumnStore;

[RegisterCopier]
internal sealed class ColumnSegmentStatsBuilderCopier : IDeepCopier<ColumnSegmentStats.Builder>
{
    public ColumnSegmentStats.Builder DeepCopy(ColumnSegmentStats.Builder input, CopyContext context)
    {
        var builder = ColumnSegmentStats.CreateBuilder();

        builder.Name = input.Name;
        builder.RowCount = input.RowCount;
        builder.RangeCount = input.RangeCount;
        builder.DistinctValueCount = input.DistinctValueCount;
        builder.DefaultValueCount = input.DefaultValueCount;

        return builder;
    }
}