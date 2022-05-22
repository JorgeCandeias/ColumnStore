using Orleans;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
[Immutable]
internal readonly record struct ColumnSegmentRange(
    [property: Id(1)] int Start,
    [property: Id(2)] int End);

[Immutable]
[GenerateSerializer]
internal readonly record struct ColumnSegmentHeader<TValue>(
    [property: Id(1)] TValue Value,
    [property: Id(2)] int RangeCount);