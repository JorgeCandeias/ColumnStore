using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class SolidRowGroupGenerator
{
    public static MemberDeclarationSyntax Generate(ColumnStoreTypeDescription type, LibraryTypes library)
    {
        var generatedTypeName = $"{type.Symbol.Name}{library.SolidRowGroup.Name}";
        var baseTypeName = $"{library.SolidRowGroup.Name}<{type.Symbol.Name}>";

        var code = $@"

            namespace {type.GeneratedNamespace}
            {{
                [GeneratedCode(""{nameof(SolidRowGroupGenerator)}"", null)]
                [GenerateSerializer]
                internal class {generatedTypeName}: {baseTypeName}
                {{
                    public {generatedTypeName}(int id) : base(id)
                    {{
                    }}

                    {type.Properties.Render(p => $"private readonly {library.IColumnSegment.Name}<{p.Type.ToDisplayString()}> _{p.Name}Segment;")}

                    public override RowGroupStats GetStats()
                    {{
                        throw new NotImplementedException();
                    }}

                    public override IEnumerator<{type.Symbol.ToDisplayString()}> GetEnumerator()
                    {{
                        {type.Properties.Render(p => $"var {p.Name}Enumerator = _{p.Name}Segment.GetEnumerator();")}

                        while ({type.Properties.Render(p => $"{p.Name}Enumerator.MoveNext()", " && ")})
                        {{
                            yield return new {type.Symbol.ToDisplayString()}
                            {{
                                {type.Properties.Render(p => $"{p.Name} = {p.Name}Enumerator.Current,")}
                            }};
                        }}
                    }}
                }}
            }}
        ";

        return SyntaxFactory.ParseCompilationUnit(code).ChildNodes().Cast<MemberDeclarationSyntax>().First();
    }
}