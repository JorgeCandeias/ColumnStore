using Orleans;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.Serializers;

namespace Outcompute.ColumnStore;

[RegisterSerializer]
internal sealed class ColumnSegmentStatsBuilderCodec : GeneralizedReferenceTypeSurrogateCodec<ColumnSegmentStats.Builder, ColumnSegmentStatsSurrogate>
{
    public ColumnSegmentStatsBuilderCodec(IValueSerializer<ColumnSegmentStatsSurrogate> surrogateSerializer) : base(surrogateSerializer)
    {
    }

    public override ColumnSegmentStats.Builder ConvertFromSurrogate(ref ColumnSegmentStatsSurrogate surrogate)
    {
        var builder = ColumnSegmentStats.CreateBuilder();

        builder.Name = surrogate.Name;
        builder.RowCount = surrogate.RowCount;
        builder.RangeCount = surrogate.RangeCount;
        builder.DistinctValueCount = surrogate.DistinctValueCount;
        builder.DefaultValueCount = surrogate.DefaultValueCount;

        return builder;
    }

    public override void ConvertToSurrogate(ColumnSegmentStats.Builder value, ref ColumnSegmentStatsSurrogate surrogate)
    {
        surrogate = new ColumnSegmentStatsSurrogate(
            value.Name,
            value.RowCount,
            value.RangeCount,
            value.DistinctValueCount,
            value.DefaultValueCount);
    }
}

[GenerateSerializer]
internal record struct ColumnSegmentStatsSurrogate(
    [property: Id(1)] string Name,
    [property: Id(2)] int RowCount,
    [property: Id(3)] int RangeCount,
    [property: Id(4)] int DistinctValueCount,
    [property: Id(5)] int DefaultValueCount);