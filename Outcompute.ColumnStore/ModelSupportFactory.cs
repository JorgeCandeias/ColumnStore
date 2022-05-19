using Outcompute.ColumnStore.CodeGeneration;
using System.Collections.Concurrent;

namespace Outcompute.ColumnStore;

/// <summary>
/// Creates the support classes based on user types.
/// </summary>
internal class ModelSupportFactory
{
    //private readonly ModelCodeGenerator _generator = new();

    private readonly ConcurrentDictionary<Type, ModelSupportTypes> _lookup = new();

    private readonly Func<Type, ModelSupportTypes> _delegate;// = (Type model) => _generator.CreateTypes(model);

    public CompressedRowGroup<TRow> Create<TRow>()
    {
        var types = GetOrAddTypes<TRow>();

        return (CompressedRowGroup<TRow>)Activator.CreateInstance(types.CompressedRowGroupType);
    }

    public void ReadyTypes<TRow>()
    {
        GetOrAddTypes<TRow>();
    }

    private ModelSupportTypes GetOrAddTypes<TRow>()
    {
        return _lookup.GetOrAdd(typeof(TRow), _delegate);
    }

    public ModelSupportFactory Default { get; } = new();
}