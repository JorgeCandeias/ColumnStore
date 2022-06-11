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
        builder.DistinctValueCount = surrogate.DistinctValueCount;
        builder.DefaultValueCount = surrogate.DefaultValueCount;

        return builder;
    }

    public override void ConvertToSurrogate(ColumnSegmentStats.Builder value, ref ColumnSegmentStatsSurrogate surrogate)
    {
        surrogate = new ColumnSegmentStatsSurrogate(
            value.Name,
            value.DistinctValueCount,
            value.DefaultValueCount);
    }
}

[GenerateSerializer]
internal record struct ColumnSegmentStatsSurrogate(
    [property: Id(1)] string Name,
    [property: Id(2)] int DistinctValueCount,
    [property: Id(3)] int DefaultValueCount);