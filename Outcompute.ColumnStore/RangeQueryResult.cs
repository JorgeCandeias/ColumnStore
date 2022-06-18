namespace Outcompute.ColumnStore;

internal record struct RangeQueryResult<TValue>(uint Start, uint End, TValue Value);

internal record struct RangeQueryResult(uint Start, uint End);