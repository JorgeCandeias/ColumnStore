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
                [Orleans.GenerateSerializer]
                #pragma warning disable CS0618
                internal class {generatedTypeName}: {baseTypeName}
                #pragma warning restore CS0618
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
                            _updated = true;
                        }}
                        ")}

                        if (_updated)
                        {{
                            _stats.Clear();
                        }}
                    }}

                    [OnDeserialized]
                    protected void OnDeserialized(StreamingContext context)
                    {{
                        _updated = true;
                    }}

                    public override IReadOnlyDictionary<string, DeltaColumnStats> GetStats()
                    {{
                        if (_updated)
                        {{
                            {type.Properties.Render(p => $@"_stats[""{p.Name}""] = _{p.Name}Stats.ToImmutable();")}

                            _updated = false;
                        }}

                        return _stats.ToImmutable();
                    }}

                    {type.Properties.Render(p => $"private readonly {library.HashSet.Name}<{p.Type.ToDisplayString()}> _{p.Name}Set = new();")}

                    {type.Properties.Render(p => $"private readonly DeltaColumnStats.Builder _{p.Name}Stats = DeltaColumnStats.CreateBuilder();")}

                    private readonly ImmutableDictionary<string, DeltaColumnStats>.Builder _stats = ImmutableDictionary.CreateBuilder<string, DeltaColumnStats>();

                    private bool _updated;
                }}
            }}
        ";

        if (SyntaxFactory.ParseCompilationUnit(code).ChildNodes().First() is not MemberDeclarationSyntax syntax)
        {
            throw new InvalidOperationException($"Could not generate type '{generatedTypeName}' for {type.Symbol.ToDisplayString()}");
        }

        return syntax;
    }
}