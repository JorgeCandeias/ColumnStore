using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Outcompute.ColumnStore.CodeGenerator
{
    /// <summary>
    /// Identifies types annotated with the column store attribute.
    /// </summary>
    internal class ColumnStoreSyntaxContextReceiver : ISyntaxContextReceiver
    {
        private LibraryTypes? _library;

        public void OnVisitSyntaxNode(GeneratorSyntaxContext context)
        {
            // only look at classes, structs or records
            if (context.Node is not ClassDeclarationSyntax && context.Node is not StructDeclarationSyntax && context.Node is not RecordDeclarationSyntax)
            {
                return;
            }

            // lazily build the type lookup
            _library ??= new LibraryTypes(context.SemanticModel.Compilation);

            // analyse the candidate
            if (context.SemanticModel.GetDeclaredSymbol(context.Node) is INamedTypeSymbol symbol)
            {
                foreach (var attribute in symbol.GetAttributes())
                {
                    if (attribute.AttributeClass?.Equals(_library.ColumnStoreAttribute, SymbolEqualityComparer.Default) ?? false)
                    {
                        Model.ColumnStoreTypes.Add(symbol);
                    }
                }
            }
        }

        public SourceModel Model { get; } = new();
    }
}