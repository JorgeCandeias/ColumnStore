using Microsoft.Extensions.DependencyInjection;

namespace Outcompute.ColumnStore;

internal class SolidRowGroupFactory<TRow> : ISolidRowGroupFactory<TRow>
{
    private readonly ISolidRowGroupFactory<TRow> _factory;

    public SolidRowGroupFactory(IServiceProvider provider)
    {
        Guard.IsNotNull(provider, nameof(provider));

        var modelType = typeof(TRow);
        var generatedFactoryName = $"{modelType.Namespace}.GeneratedCode.{modelType.Name}SolidRowGroupFactory";

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            if (assembly.GetType(generatedFactoryName) is Type generatedType)
            {
                // create and hold an instance of the generated factory for this user model
                _factory = (ISolidRowGroupFactory<TRow>)ActivatorUtilities.CreateInstance(provider, generatedType);
                break;
            }
        }

        if (_factory is null)
        {
            throw new InvalidOperationException($"Unabled to find generated type '{generatedFactoryName}' for {nameof(ColumnStore)} user model '{modelType.ToTypeString()}'");
        }
    }

    public ISolidRowGroup<TRow> Create(IRowGroup<TRow> source)
    {
        // defer to the generated factory
        return _factory.Create(source);
    }
}