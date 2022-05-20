using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class IgnoreAccessChecksToAttributeGenerator
{
    public static AttributeListSyntax[] GenerateAttributes()
    {
        var code = $@"
            [assembly: IgnoresAccessChecksTo(""Outcompute.ColumnStore.DeltaRowGroup<TRow>"")]
            [assembly: IgnoresAccessChecksTo(""Outcompute.ColumnStore.Abstractions"")]
        ";

        return SyntaxFactory.ParseCompilationUnit(code).ChildNodes().Cast<AttributeListSyntax>().ToArray();
    }

    public static MemberDeclarationSyntax GenerateMembers()
    {
        var code = $@"
            namespace System.Runtime.CompilerServices
            {{
                [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
                public sealed class IgnoresAccessChecksToAttribute : Attribute
                {{
                    public IgnoresAccessChecksToAttribute(string assemblyName)
                    {{
                        AssemblyName = assemblyName;
                    }}

                    public string AssemblyName {{ get; }}
                }}
            }}
        ";

        return SyntaxFactory.ParseCompilationUnit(code).ChildNodes().Cast<MemberDeclarationSyntax>().Single();
    }
}