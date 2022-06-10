using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class SolidRowGroupFactoryGenerator
{
    public static MemberDeclarationSyntax Generate(Model type, LibraryTypes library)
    {
        var generatedTypeName = $"{type.Symbol.Name}{library.SolidRowGroupFactory.Name}";
        var baseTypeName = library.ISolidRowGroupFactory.ToDisplayString().Replace("<TRow>", $"<{type.Symbol.ToDisplayString()}>");

        var code = $@"

            namespace {type.GeneratedNamespace}
            {{
                [GeneratedCode(""{nameof(SolidRowGroupFactoryGenerator)}"", null)]
                [GenerateSerializer]
                internal class {generatedTypeName}: {baseTypeName}
                {{
                    {type.Properties.Render(p => $"private readonly {library.IColumnSegmentBuilderFactory.Name}<{p.Type.ToDisplayString()}> _{p.Name}{library.ColumnSegmentBuilderFactory.Name};")}

                    {type.Properties.Render(p => $"private readonly Func<{type.Symbol.ToDisplayString()}, {p.Type.ToDisplayString()}> _{p.Name}Selector = ({type.Symbol.ToDisplayString()} x) => x.{p.Name};")}

                    public {generatedTypeName}({type.Properties.Render(p => $"{library.IColumnSegmentBuilderFactory.Name}<{p.Type.ToDisplayString()}> {p.Name}{library.ColumnSegmentBuilderFactory.Name}", ",")})
                    {{
                        {type.Properties.Render(p => $"_{p.Name}{library.ColumnSegmentBuilderFactory.Name} = {p.Name}{library.ColumnSegmentBuilderFactory.Name};")}
                    }}

                    public ISolidRowGroup<{type.Symbol.ToDisplayString()}> Create({library.IRowGroup.Name}<{type.Symbol.ToDisplayString()}> rows)
                    {{
                        // create segment builders for each segment
                        {type.Properties.Render(p => $"var {p.Name}{library.ColumnSegmentBuilder.Name} = _{p.Name}{library.ColumnSegmentBuilderFactory.Name}.Create(Comparer<{p.Type.ToDisplayString()}>.Default);")}

                        // get source stats to optimize ordering
                        var order = rows.Stats.ColumnSegmentStats.Values.OrderBy(x => x.DistinctValueCount).Select(x => x.Name).ToList();

                        // order the source data by cardinality to optimize compression
                        IOrderedEnumerable<{type.Symbol.ToDisplayString()}> ordered = null!;
                        var first = true;
                        foreach (var property in order)
                        {{
                            switch (property)
                            {{
                                {type.Properties.Render(p => $@"
                                case ""{p.Name}"":
                                    ordered = first ? rows.OrderBy(_{p.Name}Selector) : ordered.ThenBy(_{p.Name}Selector);
                                    break;
                                ")}
                            }}

                            first = false;
                        }}

                        // populate the column segments from the source data
                        foreach (var item in ordered)
                        {{
                            {type.Properties.Render(p => $"{p.Name}{library.ColumnSegmentBuilder.Name}.Add(item.{p.Name});")}
                        }}

                        // close the column segments
                        {type.Properties.Render(p => $"var {p.Name}{library.ColumnSegment.Name} = {p.Name}{library.ColumnSegmentBuilder.Name}.ToImmutable();")}

                        // create the solid row group
                        return new {type.Symbol.Name}{library.SolidRowGroup.Name}(rows.Id, rows.Stats, {type.Properties.Render(p => $"{p.Name}{library.ColumnSegment.Name}", ",")});
                    }}
                }}
            }}
        ";

        return SyntaxFactory.ParseCompilationUnit(code).ChildNodes().Cast<MemberDeclarationSyntax>().First();
    }
}