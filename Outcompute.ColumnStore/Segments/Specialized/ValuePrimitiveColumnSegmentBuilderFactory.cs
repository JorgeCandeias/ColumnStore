using Microsoft.Extensions.DependencyInjection;
using Outcompute.ColumnStore.ColumnSegments;

namespace Outcompute.ColumnStore.Segments.Specialized;

internal class ValuePrimitiveColumnSegmentBuilderFactory<T> : ColumnSegmentBuilderFactory<T> where T : struct
{
    public ValuePrimitiveColumnSegmentBuilderFactory(IServiceProvider serviceProvider) : base(serviceProvider)
    {
    }

    public override ColumnSegmentBuilder<T> Create(IComparer<T> comparer)
    {
        return ActivatorUtilities.CreateInstance<ValuePrimitiveColumnSegmentBuilder<T>>(ServiceProvider, comparer);
    }
}