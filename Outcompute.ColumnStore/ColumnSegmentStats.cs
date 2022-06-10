using Orleans;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.Serializers;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public record ColumnSegmentStats(
    [property: Id(1)] string Name,
    [property: Id(2)] int DistinctValueCount,
    [property: Id(3)] int DefaultValueCount)
    : IColumnSegmentStats
{
    public class Builder
    {
        internal Builder()
        {
        }

        public string Name { get; set; } = "";

        public int DistinctValueCount { get; set; }

        public int DefaultValueCount { get; set; }

        public ColumnSegmentStats ToImmutable() => new(Name, DistinctValueCount, DefaultValueCount);
    }

    public static Builder CreateBuilder() => new();
}

[GenerateSerializer]
internal record struct ColumnSegmentStatsSurrogate(
    [property: Id(1)] string Name,
    [property: Id(2)] int DistinctValueCount,
    [property: Id(3)] int DefaultValueCount);

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

[RegisterCopier]
internal sealed class ColumnSegmentStatsBuilderCopier : IDeepCopier<ColumnSegmentStats.Builder>
{
    public ColumnSegmentStats.Builder DeepCopy(ColumnSegmentStats.Builder input, CopyContext context)
    {
        var builder = ColumnSegmentStats.CreateBuilder();

        builder.Name = input.Name;
        builder.DistinctValueCount = input.DistinctValueCount;
        builder.DefaultValueCount = input.DefaultValueCount;

        return builder;
    }
}