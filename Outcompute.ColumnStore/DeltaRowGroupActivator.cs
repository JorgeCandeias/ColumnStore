using Orleans;
using Orleans.Serialization.Activators;

namespace Outcompute.ColumnStore;

[RegisterActivator]
internal class DeltaRowGroupActivator<TRow> : IActivator<DeltaRowGroup<TRow>>
{
    private readonly DeltaRowGroupFactory<TRow> _factory;

    public DeltaRowGroupActivator(DeltaRowGroupFactory<TRow> factory)
    {
        _factory = factory;
    }

    public DeltaRowGroup<TRow> Create()
    {
        return _factory.Create(0, 0);
    }
}