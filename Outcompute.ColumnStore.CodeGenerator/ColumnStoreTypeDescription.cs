using Microsoft.CodeAnalysis;

namespace Outcompute.ColumnStore.CodeGenerator;

internal class ColumnStoreTypeDescription
{
    public string GeneratedNamespace { get; set; } = "";

    public INamedTypeSymbol Symbol { get; set; } = null!;

    public IList<IPropertySymbol> Properties { get; } = new List<IPropertySymbol>();
}