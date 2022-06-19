namespace Outcompute.ColumnStore;

internal record struct RangeQueryResult<TValue>(int Start, int End, TValue Value);

internal record struct RangeQueryResult(int Start, int End);