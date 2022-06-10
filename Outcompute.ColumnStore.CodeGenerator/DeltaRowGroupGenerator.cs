using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Reflection;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class DeltaRowGroupGenerator
{
    public static MemberDeclarationSyntax Generate(ColumnStoreTypeDescription type, LibraryTypes library)
    {
        var generatedTypeName = $"{type.Symbol.Name}{library.DeltaRowGroup1.Name}";

        var code = $@"

            namespace {type.GeneratedNamespace}
            {{
                [{library.GeneratedCodeAttribute}(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [{library.GenerateSerializerAttribute}]
                [{library.UseActivatorAttribute}]
                internal class {generatedTypeName}: {library.DeltaRowGroup1.Construct(type.Symbol)}
                {{
                    {type.Properties.Render(p => $"private readonly {library.HashSet.Construct(p.Type)} _{p.Name}Set = new();")}

                    {type.Properties.Render(p => $"private readonly {library.ColumnSegmentStatsBuilder} _{p.Name}Stats = {library.ColumnSegmentStats}.CreateBuilder();")}

                    public {generatedTypeName}(
                        {library.Int32} id,
                        {library.Int32} capacity,
                        {library.Serializer1.Construct(type.Symbol)} serializer,
                        {library.SerializerSessionPool} sessions)
                        : base(id, capacity, serializer, sessions)
                    {{
                        {type.Properties.Render(p => @$"_{p.Name}Stats.Name = ""{p.Name}"";")}
                    }}

                    protected override void OnAdded({type.Symbol} row)
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

                    protected override void OnBuildStats({library.RowGroupStatsBuilder} builder)
                    {{
                        {type.Properties.Render(p => $@"builder.ColumnSegmentStats[""{p.Name}""] = _{p.Name}Stats.ToImmutable();")}
                    }}
                }}

                [{library.GeneratedCodeAttribute}(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [{library.RegisterDeltaRowFactoryAttribute}(typeof({type.Symbol}))]
                internal class {generatedTypeName}Factory: {library.DeltaRowGroupFactory1.Construct(type.Symbol)}
                {{
                    private readonly {library.ObjectFactory} _factory;

                    public {generatedTypeName}Factory({library.IServiceProvider} serviceProvider): base(serviceProvider)
                    {{
                        _factory = {library.ActivatorUtilities}.CreateFactory(typeof({generatedTypeName}), new[] {{ typeof({library.Int32}), typeof({library.Int32})}});
                    }}

                    public override {generatedTypeName} Create({library.Int32} id, {library.Int32} capacity)
                    {{
                        return ({generatedTypeName}) _factory.Invoke(ServiceProvider, new object[] {{ id, capacity }});
                    }}
                }}

                [{library.GeneratedCodeAttribute}(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [{library.RegisterActivatorAttribute}]
                internal class {generatedTypeName}Activator:
                    {library.IActivator1.ToDisplayString().Replace("<T>", $"<{generatedTypeName}>")},
                    {library.IActivator1.ToDisplayString().Replace("<T>", $"<{library.DeltaRowGroup1.Construct(type.Symbol)}>")}
                {{
                    private readonly {generatedTypeName}Factory _factory;

                    public {generatedTypeName}Activator({generatedTypeName}Factory factory)
                    {{
                        _factory = factory;
                    }}

                    public {generatedTypeName} Create()
                    {{
                        return _factory.Create(0, 0);
                    }}

                    {library.DeltaRowGroup1.Construct(type.Symbol)} {library.IActivator1.ToDisplayString().Replace("<T>", $"<{library.DeltaRowGroup1.Construct(type.Symbol)}>")}.Create() => Create();
                }}
            }}
        ";

        return SyntaxFactory.ParseCompilationUnit(code).ChildNodes().Cast<MemberDeclarationSyntax>().First();
    }
}