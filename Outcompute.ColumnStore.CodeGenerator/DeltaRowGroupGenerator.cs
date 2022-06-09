using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Reflection;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class DeltaRowGroupGenerator
{
    public static MemberDeclarationSyntax Generate(ColumnStoreTypeDescription type, LibraryTypes library)
    {
        var generatedTypeName = $"{type.Symbol.Name}{library.DeltaRowGroup.Name}";
        var baseTypeName = library.DeltaRowGroup.ToDisplayString().Replace("<TRow>", $"<{type.Symbol.ToDisplayString()}>");

        var code = $@"

            namespace {type.GeneratedNamespace}
            {{
                [GeneratedCode(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [GenerateSerializer]
                [UseActivator]
                internal class {generatedTypeName}: {baseTypeName}
                {{
                    private {generatedTypeName}()
                    {{
                    }}

                    {type.Properties.Render(p => $"private readonly {library.HashSet.Name}<{p.Type.ToDisplayString()}> _{p.Name}Set = new();")}

                    {type.Properties.Render(p => $"private readonly ColumnSegmentStats.Builder _{p.Name}Stats = ColumnSegmentStats.CreateBuilder();")}

                    public {generatedTypeName}(int id, {library.ColumnStoreOptions.Name} options, {library.Serializer1.ToDisplayString().Replace("<T>", $"<{type.Symbol.ToDisplayString()}>")} serializer, {library.SerializerSessionPool.ToDisplayString()} sessions) : base(id, options, serializer, sessions)
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

                    protected override void OnBuildStats(RowGroupStats.Builder builder)
                    {{
                        {type.Properties.Render(p => $@"builder.ColumnSegmentStats[""{p.Name}""] = _{p.Name}Stats.ToImmutable();")}
                    }}
                }}

                [RegisterActivator]
                internal class {generatedTypeName}Activator: IActivator<{generatedTypeName}>
                {{
                    private readonly 
                    public {generatedTypeName}Activator()
                    {{                        
                    }}

                    public {generatedTypeName} Create()
                    {{  
                    }}
                }}
            }}
        ";

        return SyntaxFactory.ParseCompilationUnit(code).ChildNodes().Cast<MemberDeclarationSyntax>().First();
    }
}