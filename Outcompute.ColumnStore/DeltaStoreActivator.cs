using Orleans;
using Orleans.Serialization.Activators;

namespace Outcompute.ColumnStore;

[RegisterActivator]
internal class DeltaStoreActivator<TRow> : IActivator<DeltaStore<TRow>>
{
    private readonly DeltaStoreFactory<TRow> _factory;

    public DeltaStoreActivator(DeltaStoreFactory<TRow> factory)
    {
        Guard.IsNotNull(factory, nameof(factory));

        _factory = factory;
    }

    public DeltaStore<TRow> Create() => _factory.Create(0);
}