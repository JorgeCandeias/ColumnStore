using Microsoft.CodeAnalysis;

namespace Outcompute.ColumnStore.CodeGenerator;

internal class Model
{
    public string GeneratedNamespace { get; set; } = "";

    public INamedTypeSymbol Symbol { get; set; } = null!;

    public IList<IPropertySymbol> Properties { get; } = new List<IPropertySymbol>();

    public IList<ushort> Ids { get; } = new List<ushort>();
}