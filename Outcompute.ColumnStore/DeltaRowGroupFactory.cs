﻿namespace Outcompute.ColumnStore;

public abstract class DeltaRowGroupFactory<TRow>
{
    protected DeltaRowGroupFactory(IServiceProvider serviceProvider)
    {
        Guard.IsNotNull(serviceProvider, nameof(serviceProvider));

        ServiceProvider = serviceProvider;
    }

    protected IServiceProvider ServiceProvider { get; }

    public abstract DeltaRowGroup<TRow> Create(int id, int capacity);
}