using Microsoft.CodeAnalysis;

namespace Outcompute.ColumnStore.CodeGenerator;

internal class LibraryTypes
{
    private readonly Compilation _compilation;
    private readonly Dictionary<string, INamedTypeSymbol> _symbols = new();

    public LibraryTypes(Compilation compilation)
    {
        _compilation = compilation;
    }

    public Compilation Compilation { get; private set; } = null!;

    public INamedTypeSymbol ActivatorUtilities => Type("Microsoft.Extensions.DependencyInjection.ActivatorUtilities");
    public INamedTypeSymbol ColumnSegment => Type("Outcompute.ColumnStore.ColumnSegment`1");
    public INamedTypeSymbol ColumnSegmentBuilder => Type("Outcompute.ColumnStore.ColumnSegmentBuilder`1");
    public INamedTypeSymbol ColumnSegmentBuilderFactory => Type("Outcompute.ColumnStore.ColumnSegmentBuilderFactory`1");
    public INamedTypeSymbol ColumnSegmentStats => Type("Outcompute.ColumnStore.ColumnSegmentStats");
    public INamedTypeSymbol ColumnSegmentStatsBuilder => Type("Outcompute.ColumnStore.ColumnSegmentStats+Builder");
    public INamedTypeSymbol ColumnStoreAttribute => Type("Outcompute.ColumnStore.ColumnStoreAttribute");
    public INamedTypeSymbol ColumnStoreOptions => Type("Outcompute.ColumnStore.ColumnStoreOptions");
    public INamedTypeSymbol ColumnStorePropertyAttribute => Type("Outcompute.ColumnStore.ColumnStorePropertyAttribute");
    public INamedTypeSymbol DeltaRowGroup1 => Type("Outcompute.ColumnStore.DeltaRowGroup`1");
    public INamedTypeSymbol DeltaRowGroupFactory1 => Type("Outcompute.ColumnStore.DeltaRowGroupFactory`1");
    public INamedTypeSymbol GeneratedCodeAttribute => Type("System.CodeDom.Compiler.GeneratedCodeAttribute");
    public INamedTypeSymbol GenerateSerializerAttribute => Type("Orleans.GenerateSerializerAttribute");
    public INamedTypeSymbol HashSet => Type("System.Collections.Generic.HashSet`1");
    public INamedTypeSymbol IColumnSegment => Type("Outcompute.ColumnStore.IColumnSegment`1");
    public INamedTypeSymbol IColumnSegmentBuilder => Type("Outcompute.ColumnStore.IColumnSegmentBuilder`1");
    public INamedTypeSymbol IColumnSegmentBuilderFactory => Type("Outcompute.ColumnStore.IColumnSegmentBuilderFactory`1");
    public INamedTypeSymbol IDeltaRowGroup1 => Type("Outcompute.ColumnStore.IDeltaRowGroup`1");
    public INamedTypeSymbol IDeltaRowGroupFactory1 => Type("Outcompute.ColumnStore.IDeltaRowGroupFactory`1");
    public INamedTypeSymbol Int32 => Type("System.Int32");
    public INamedTypeSymbol IOptions => Type("Microsoft.Extensions.Options.IOptions`1");
    public INamedTypeSymbol IRowGroup => Type("Outcompute.ColumnStore.IRowGroup`1");
    public INamedTypeSymbol IServiceProvider => Type("System.IServiceProvider");
    public INamedTypeSymbol ISolidRowGroupFactory => Type("Outcompute.ColumnStore.ISolidRowGroupFactory`1");
    public INamedTypeSymbol ObjectFactory => Type("Microsoft.Extensions.DependencyInjection.ObjectFactory");
    public INamedTypeSymbol RegisterActivatorAttribute => Type("Orleans.RegisterActivatorAttribute");
    public INamedTypeSymbol RegisterDeltaRowFactoryAttribute => Type("Outcompute.ColumnStore.RegisterDeltaRowFactoryAttribute");
    public INamedTypeSymbol RowGroupStats => Type("Outcompute.ColumnStore.RowGroupStats");
    public INamedTypeSymbol RowGroupStatsBuilder => Type("Outcompute.ColumnStore.RowGroupStats+Builder");
    public INamedTypeSymbol Serializer1 => Type("Orleans.Serialization.Serializer`1");
    public INamedTypeSymbol SerializerSessionPool => Type("Orleans.Serialization.Session.SerializerSessionPool");
    public INamedTypeSymbol SolidRowGroup => Type("Outcompute.ColumnStore.SolidRowGroup`1");
    public INamedTypeSymbol SolidRowGroupFactory => Type("Outcompute.ColumnStore.SolidRowGroupFactory`1");
    public INamedTypeSymbol UseActivatorAttribute => Type("Orleans.UseActivatorAttribute");
    public INamedTypeSymbol IActivator1 => Type("Orleans.Serialization.Activators.IActivator`1");

    private INamedTypeSymbol Type(string fullyQualifiedMetadataName)
    {
        if (!_symbols.TryGetValue(fullyQualifiedMetadataName, out var symbol))
        {
            if (_compilation.GetTypeByMetadataName(fullyQualifiedMetadataName) is not INamedTypeSymbol found)
            {
                throw new InvalidOperationException($"Cannot find type with metadata name '{fullyQualifiedMetadataName}'");
            }

            _symbols[fullyQualifiedMetadataName] = symbol = found;
        }

        return symbol;
    }
}