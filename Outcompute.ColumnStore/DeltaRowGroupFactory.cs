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

        var modelType = typeof(TRow);
        var generatedTypeName = $"{modelType.Namespace}.ColumnStoreCodeGen.{modelType.Name}DeltaRowGroup";

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            if (assembly.GetType(generatedTypeName) is Type generatedType)
            {
                _factory = ActivatorUtilities.CreateFactory(generatedType, new[] { typeof(int) });
                break;
            }
        }

        if (_factory is null)
        {
            throw new InvalidOperationException($"Unabled to find generated type '{generatedTypeName}' for {nameof(ColumnStore)} user model '{modelType.ToTypeString()}'");
        }
    }

    public IDeltaRowGroup<TRow> Create(int id) => (IDeltaRowGroup<TRow>)_factory.Invoke(_provider, new object[] { id });
}