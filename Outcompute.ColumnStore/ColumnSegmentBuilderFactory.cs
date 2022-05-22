using Microsoft.Extensions.DependencyInjection;

namespace Outcompute.ColumnStore;

internal class ColumnSegmentBuilderFactory<TValue> : IColumnSegmentBuilderFactory<TValue>
{
    private readonly IServiceProvider _provider;
    private readonly ObjectFactory _factory;

    public ColumnSegmentBuilderFactory(IServiceProvider provider)
    {
        _provider = provider;
        _factory = ActivatorUtilities.CreateFactory(typeof(ColumnSegmentBuilder<TValue>), new[] { typeof(IComparer<TValue>) });
    }

    public IColumnSegmentBuilder<TValue> Create(IComparer<TValue> comparer)
    {
        Guard.IsNotNull(comparer, nameof(comparer));

        return (IColumnSegmentBuilder<TValue>)_factory(_provider, new object[] { comparer });
    }
}