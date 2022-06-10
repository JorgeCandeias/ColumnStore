using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace Outcompute.ColumnStore.CodeGenerator
{
    internal record struct SourceResult(SyntaxTree Tree, SourceText Text, string Name);
}