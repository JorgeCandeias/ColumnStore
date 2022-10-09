namespace Outcompute.ColumnStore.Sequences;

internal record Stats<T>(int Count, int DefaultCount, T Min, T Max);
