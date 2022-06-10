using Microsoft.Extensions.DependencyInjection;

namespace Outcompute.ColumnStore;

internal class DeltaStoreFactory<TRow> : IDeltaStoreFactory<TRow>
{
    private readonly IServiceProvider _provider;
    private readonly ObjectFactory _factory;

    public DeltaStoreFactory(IServiceProvider provider)
    {
        Guard.IsNotNull(provider, nameof(provider));

        _provider = provider;
        _factory = ActivatorUtilities.CreateFactory(typeof(DeltaStore<TRow>), new[] { typeof(ColumnStoreOptions) });
    }

    public IDeltaStore<TRow> Create(int rowGroupCapacity)
    {
        return (IDeltaStore<TRow>)_factory.Invoke(_provider, new object[] { rowGroupCapacity });
    }
}