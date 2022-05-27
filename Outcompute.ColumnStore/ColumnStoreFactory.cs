using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Outcompute.ColumnStore;

internal class ColumnStoreFactory<TRow> : IColumnStoreFactory<TRow>
    where TRow : new()
{
    private readonly IServiceProvider _provider;
    private readonly ObjectFactory _factory0;
    private readonly ObjectFactory _factory1;

    public ColumnStoreFactory(IServiceProvider provider)
    {
        Guard.IsNotNull(provider, nameof(provider));

        _provider = provider;
        _factory0 = ActivatorUtilities.CreateFactory(typeof(ColumnStore<TRow>), Array.Empty<Type>());
        _factory1 = ActivatorUtilities.CreateFactory(typeof(ColumnStore<TRow>), new[] { typeof(IOptions<ColumnStoreOptions>) });
    }

    public IColumnStore<TRow> Create()
    {
        return (IColumnStore<TRow>)_factory0.Invoke(_provider, null);
    }

    public IColumnStore<TRow> Create(ColumnStoreOptions options)
    {
        return (IColumnStore<TRow>)_factory1.Invoke(_provider, new[] { Options.Create(options) });
    }
}