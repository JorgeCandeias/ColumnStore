using Orleans;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.Serializers;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
public record struct ColumnSegmentStats(
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

        public Builder Clone() => new()
        {
            Name = Name,
            DistinctValueCount = DistinctValueCount,
            DefaultValueCount = DefaultValueCount
        };
    }

    public Builder ToBuilder() => new()
    {
        Name = Name,
        DistinctValueCount = DistinctValueCount,
        DefaultValueCount = DefaultValueCount
    };

    public static Builder CreateBuilder() => new();
}

[RegisterSerializer]
internal sealed class ColumnSegmentStatsBuilderCodec : GeneralizedReferenceTypeSurrogateCodec<ColumnSegmentStats.Builder, ColumnSegmentStats>
{
    public ColumnSegmentStatsBuilderCodec(IValueSerializer<ColumnSegmentStats> surrogateSerializer) : base(surrogateSerializer)
    {
    }

    public override ColumnSegmentStats.Builder ConvertFromSurrogate(ref ColumnSegmentStats surrogate)
    {
        return surrogate.ToBuilder();
    }

    public override void ConvertToSurrogate(ColumnSegmentStats.Builder value, ref ColumnSegmentStats surrogate)
    {
        surrogate = value.ToImmutable();
    }
}

[RegisterCopier]
internal sealed class ColumnSegmentStatsBuilderCopier : IDeepCopier<ColumnSegmentStats.Builder>
{
    public ColumnSegmentStats.Builder DeepCopy(ColumnSegmentStats.Builder input, CopyContext context)
    {
        return input.Clone();
    }
}