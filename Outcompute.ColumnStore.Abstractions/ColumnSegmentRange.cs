using Orleans;

namespace Outcompute.ColumnStore;

[GenerateSerializer]
[Immutable]
internal record struct ColumnSegmentRange<TValue>(

    [property: Id(1)]
    TValue Value,

    [property: Id(1)]
    int Start,

    [property: Id(2)]
    int End);