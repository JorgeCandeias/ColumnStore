using Microsoft.CodeAnalysis.CSharp.Syntax;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class UsingsGenerator
{
    public static UsingDirectiveSyntax[] Generate()
    {
        return new UsingDirectiveSyntax[]
        {
            UsingDirective(ParseName("System")),
            UsingDirective(ParseName("System.Collections.Generic")),
            UsingDirective(ParseName("Outcompute.ColumnStore")),
            UsingDirective(ParseName("CommunityToolkit.Diagnostics")),
            UsingDirective(ParseName("System.Runtime.CompilerServices")),
            UsingDirective(ParseName("Microsoft.Extensions.Options")),
            UsingDirective(ParseName("System.CodeDom.Compiler")),
            UsingDirective(ParseName("System.Collections.Immutable")),
            UsingDirective(ParseName("System.Runtime.Serialization"))
        };
    }
}