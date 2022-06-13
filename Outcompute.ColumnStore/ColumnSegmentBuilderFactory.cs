using Microsoft.Extensions.DependencyInjection;

namespace Outcompute.ColumnStore;

internal class ColumnSegmentBuilderFactory<TValue>
{
    private readonly IServiceProvider _provider;
    private readonly ObjectFactory _factory;

    public ColumnSegmentBuilderFactory(IServiceProvider provider)
    {
        _provider = provider;
        _factory = ActivatorUtilities.CreateFactory(typeof(ColumnSegmentBuilder<TValue>), new[] { typeof(IComparer<TValue>) });
    }

    public ColumnSegmentBuilder<TValue> Create(IComparer<TValue> comparer)
    {
        Guard.IsNotNull(comparer, nameof(comparer));

        return (ColumnSegmentBuilder<TValue>)_factory(_provider, new object[] { comparer });
    }
}