using Outcompute.ColumnStore.Segments;

namespace Outcompute.ColumnStore.ColumnSegments;

internal abstract class ColumnSegmentBuilderFactory<T>
{
    protected ColumnSegmentBuilderFactory(IServiceProvider serviceProvider)
    {
        Guard.IsNotNull(serviceProvider, nameof(serviceProvider));

        ServiceProvider = serviceProvider;
    }

    protected IServiceProvider ServiceProvider { get; }

    public abstract ColumnSegmentBuilder<T> Create(IComparer<T> comparer);
}