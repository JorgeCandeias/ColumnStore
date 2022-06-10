using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using System.Reflection;
using System.Text;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class DeltaRowGroupGenerator
{
    public static SourceResult Generate(Compilation compilation, ColumnStoreTypeDescription type, LibraryTypes library)
    {
        var modelNamespace = type.Symbol.ContainingNamespace.ToDisplayString();
        var modelTypeName = type.Symbol.ToDisplayString();
        var modelTitle = modelTypeName.Substring(modelNamespace.Length);
        var typeName = $"{modelTitle.Replace(".", "")}{library.DeltaRowGroup1.Name}";

        var code = $@"

            namespace {type.Symbol.ContainingNamespace}.{Constants.CodeGenNamespace}
            {{
                [{library.GeneratedCodeAttribute}(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [{library.GenerateSerializerAttribute}]
                [{library.UseActivatorAttribute}]
                internal class {typeName}: {library.DeltaRowGroup1.Construct(type.Symbol)}
                {{
                    {type.Properties.Render(p => $"private readonly {library.HashSet.Construct(p.Type)} _{p.Name}Set = new();")}

                    {type.Properties.Render(p => $"private readonly {library.ColumnSegmentStatsBuilder} _{p.Name}Stats = {library.ColumnSegmentStats}.CreateBuilder();")}

                    public {typeName}(
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
                internal class {typeName}Factory: {library.DeltaRowGroupFactory1.Construct(type.Symbol)}
                {{
                    private readonly {library.ObjectFactory} _factory;

                    public {typeName}Factory({library.IServiceProvider} serviceProvider): base(serviceProvider)
                    {{
                        _factory = {library.ActivatorUtilities}.CreateFactory(typeof({typeName}), new[] {{ typeof({library.Int32}), typeof({library.Int32})}});
                    }}

                    public override {typeName} Create({library.Int32} id, {library.Int32} capacity)
                    {{
                        return ({typeName}) _factory.Invoke(ServiceProvider, new object[] {{ id, capacity }});
                    }}
                }}

                [{library.GeneratedCodeAttribute}(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [{library.RegisterActivatorAttribute}]
                internal class {typeName}Activator:
                    {library.IActivator1.ToDisplayString().Replace("<T>", $"<{typeName}>")},
                    {library.IActivator1.ToDisplayString().Replace("<T>", $"<{library.DeltaRowGroup1.Construct(type.Symbol)}>")}
                {{
                    private readonly {typeName}Factory _factory;

                    public {typeName}Activator({typeName}Factory factory)
                    {{
                        _factory = factory;
                    }}

                    public {typeName} Create()
                    {{
                        return _factory.Create(0, 0);
                    }}

                    {library.DeltaRowGroup1.Construct(type.Symbol)} {library.IActivator1.ToDisplayString().Replace("<T>", $"<{library.DeltaRowGroup1.Construct(type.Symbol)}>")}.Create() => Create();
                }}
            }}
        ";

        var tree = CSharpSyntaxTree.ParseText(code);
        var text = SourceText.From(tree.GetRoot().NormalizeWhitespace().ToFullString(), Encoding.UTF8);
        var name = $"{compilation.AssemblyName}.{Constants.CodeGenNamespace}.{typeName}.g.cs";

        return new SourceResult(tree, text, name);
    }
}