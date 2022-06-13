using Orleans;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
[Immutable]
internal readonly record struct ColumnSegmentRange(
    [property: Id(1)] int Start,
    [property: Id(2)] int End);