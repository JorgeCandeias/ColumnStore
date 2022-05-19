using Microsoft.Extensions.DependencyInjection;

namespace Outcompute.ColumnStore;

internal class DeltaRowGroupFactory<TRow> : IDeltaRowGroupFactory<TRow>
{
    private readonly IServiceProvider _provider;
    private readonly ObjectFactory _factory;

    public DeltaRowGroupFactory(IServiceProvider provider)
    {
        Guard.IsNotNull(provider, nameof(provider));

        _provider = provider;

        _factory = ActivatorUtilities.CreateFactory(typeof(DeltaRowGroup<TRow>), new[] { typeof(int) });
    }

    public IRowGroup<TRow> Create(int id) => (IRowGroup<TRow>)_factory.Invoke(_provider, new object[] { id });
}