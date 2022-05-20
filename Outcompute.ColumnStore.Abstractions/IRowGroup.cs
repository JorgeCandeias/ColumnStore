﻿namespace Outcompute.ColumnStore;

/// <summary>
/// Represents a queryable group of rows.
/// </summary>
internal interface IRowGroup<out TRow> : IReadOnlyCollection<TRow>
{
    int Id { get; }

    RowGroupState State { get; }
}