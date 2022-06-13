using Orleans;

namespace Outcompute.ColumnStore;

[Immutable]
[GenerateSerializer]
internal readonly record struct ColumnSegmentHeader<TValue>(
    [property: Id(1)] TValue Value,
    [property: Id(2)] int RangeCount);