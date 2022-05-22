using Microsoft.CodeAnalysis;

namespace Outcompute.ColumnStore.CodeGenerator;

internal class LibraryTypes
{
    private LibraryTypes()
    {
    }

    public static LibraryTypes FromCompilation(Compilation compilation)
    {
        return new LibraryTypes
        {
            Compilation = compilation,
            ColumnStoreAttribute = Type("Outcompute.ColumnStore.ColumnStoreAttribute"),
            ColumnStorePropertyAttribute = Type("Outcompute.ColumnStore.ColumnStorePropertyAttribute"),
            IRowGroup = Type("Outcompute.ColumnStore.IRowGroup`1"),
            DeltaRowGroup = Type("Outcompute.ColumnStore.DeltaRowGroup`1"),
            SolidRowGroup = Type("Outcompute.ColumnStore.SolidRowGroup`1"),
            SolidRowGroupFactory = Type("Outcompute.ColumnStore.SolidRowGroupFactory`1"),
            ISolidRowGroupFactory = Type("Outcompute.ColumnStore.ISolidRowGroupFactory`1"),
            ColumnSegment = Type("Outcompute.ColumnStore.ColumnSegment`1"),
            ColumnSegmentBuilder = Type("Outcompute.ColumnStore.ColumnSegmentBuilder`1"),
            ColumnSegmentBuilderFactory = Type("Outcompute.ColumnStore.ColumnSegmentBuilderFactory`1"),
            IColumnSegment = Type("Outcompute.ColumnStore.IColumnSegment`1"),
            IColumnSegmentBuilder = Type("Outcompute.ColumnStore.IColumnSegmentBuilder`1"),
            IColumnSegmentBuilderFactory = Type("Outcompute.ColumnStore.IColumnSegmentBuilderFactory`1"),
            ColumnStoreOptions = Type("Outcompute.ColumnStore.ColumnStoreOptions"),
            HashSet = Type("System.Collections.Generic.HashSet`1"),
            IOptions = Type("Microsoft.Extensions.Options.IOptions`1"),
        };

        INamedTypeSymbol Type(string fullyQualifiedMetadataName)
        {
            if (compilation.GetTypeByMetadataName(fullyQualifiedMetadataName) is not INamedTypeSymbol symbol)
            {
                throw new InvalidOperationException($"Cannot find type with metadata name '{fullyQualifiedMetadataName}'");
            }

            return symbol;
        }
    }

    public Compilation Compilation { get; private set; } = null!;
    public INamedTypeSymbol ColumnStoreAttribute { get; private set; } = null!;
    public INamedTypeSymbol ColumnStorePropertyAttribute { get; private set; } = null!;
    public INamedTypeSymbol IRowGroup { get; private set; } = null!;
    public INamedTypeSymbol DeltaRowGroup { get; private set; } = null!;
    public INamedTypeSymbol SolidRowGroup { get; private set; } = null!;
    public INamedTypeSymbol SolidRowGroupFactory { get; private set; } = null!;
    public INamedTypeSymbol ISolidRowGroupFactory { get; private set; } = null!;
    public INamedTypeSymbol ColumnSegment { get; private set; } = null!;
    public INamedTypeSymbol ColumnSegmentBuilder { get; private set; } = null!;
    public INamedTypeSymbol ColumnSegmentBuilderFactory { get; private set; } = null!;
    public INamedTypeSymbol IColumnSegment { get; private set; } = null!;
    public INamedTypeSymbol IColumnSegmentBuilder { get; private set; } = null!;
    public INamedTypeSymbol IColumnSegmentBuilderFactory { get; private set; } = null!;
    public INamedTypeSymbol ColumnStoreOptions { get; private set; } = null!;
    public INamedTypeSymbol HashSet { get; private set; } = null!;
    public INamedTypeSymbol IOptions { get; private set; } = null!;
}