using Microsoft.CodeAnalysis;

namespace Outcompute.ColumnStore.CodeGenerator;

internal interface ICompressedRowGroupGenerator
{
    SyntaxTree Generate();
}