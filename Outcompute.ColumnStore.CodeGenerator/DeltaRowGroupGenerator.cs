using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class DeltaRowGroupGenerator
{
    public static MemberDeclarationSyntax Generate(ColumnStoreTypeDescription type, LibraryTypes library)
    {
        var generatedTypeName = $"{type.Symbol.Name}{library.DeltaRowGroup.Name}";
        var baseTypeName = $"{library.DeltaRowGroup.Name}<{type.Symbol.Name}>";
        var optionsTypeName = $"{library.IOptions.Name}<{library.ColumnStoreOptions.Name}>";

        var code = $@"

            namespace {type.GeneratedNamespace}
            {{
                [GeneratedCode(""{nameof(DeltaRowGroupGenerator)}"", null)]
                [GenerateSerializer]
                internal class {generatedTypeName}: {baseTypeName}
                {{
                    public {generatedTypeName}(int id, {optionsTypeName} options) : base(id, options)
                    {{
                        {type.Properties.Render(p => @$"_{p.Name}Stats.Name = ""{p.Name}"";")}
                    }}

                    protected override void OnAdded({type.Symbol.ToDisplayString()} row)
                    {{
                        {type.Properties.Render(p => $@"
                        if (_{p.Name}Set.Add(row.{p.Name}))
                        {{
                            _{p.Name}Stats.DistinctValueCount = _{p.Name}Set.Count;
                        }}

                        if (row.{p.Name} == default)
                        {{
                            _{p.Name}Stats.DefaultValueCount++;
                        }}
                        ")}
                    }}

                    protected override void OnUpdateStats()
                    {{
                        {type.Properties.Render(p => $@"Stats.ColumnSegmentStats[""{p.Name}""] = _{p.Name}Stats.ToImmutable();")}
                    }}

                    {type.Properties.Render(p => $"private readonly {library.HashSet.Name}<{p.Type.ToDisplayString()}> _{p.Name}Set = new();")}

                    {type.Properties.Render(p => $"private readonly ColumnSegmentStats.Builder _{p.Name}Stats = ColumnSegmentStats.CreateBuilder();")}
                }}
            }}
        ";

        return SyntaxFactory.ParseCompilationUnit(code).ChildNodes().Cast<MemberDeclarationSyntax>().First();
    }
}