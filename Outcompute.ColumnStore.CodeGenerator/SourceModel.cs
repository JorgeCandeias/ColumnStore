using Microsoft.CodeAnalysis;

namespace Outcompute.ColumnStore.CodeGenerator;

internal class SourceModel
{
    public IList<INamedTypeSymbol> ColumnStoreTypes { get; } = new List<INamedTypeSymbol>();
}