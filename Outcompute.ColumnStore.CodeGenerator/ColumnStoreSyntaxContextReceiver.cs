using Microsoft.CodeAnalysis;

namespace Outcompute.ColumnStore.CodeGenerator
{
    /// <summary>
    /// Identifies types annotated with the column store attribute.
    /// </summary>
    internal class ColumnStoreSyntaxContextReceiver : ISyntaxContextReceiver
    {
        public SourceModel Model { get; } = new();

        public void OnVisitSyntaxNode(GeneratorSyntaxContext context)
        {
            var types = LibraryTypes.FromCompilation(context.SemanticModel.Compilation);

            if (context.SemanticModel.GetDeclaredSymbol(context.Node) is INamedTypeSymbol symbol)
            {
                foreach (var attribute in symbol.GetAttributes())
                {
                    if (attribute.AttributeClass?.Equals(types.ColumnStoreAttribute, SymbolEqualityComparer.Default) ?? false)
                    {
                        Model.ColumnStoreTypes.Add(symbol);
                    }
                }
            }
        }
    }
}