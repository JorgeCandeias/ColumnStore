using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using System.Reflection;
using System.Text;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class DeltaRowGroupGenerator
{
    public static SourceResult Generate(Compilation compilation, Model model, LibraryTypes library)
    {
        var modelNamespace = model.Symbol.ContainingNamespace.ToDisplayString();
        var modelTypeName = model.Symbol.ToDisplayString();
        var modelTitle = modelTypeName.Substring(modelNamespace.Length);
        var typeName = $"{modelTitle.Replace(".", "")}{library.DeltaRowGroup1.Name}";

        var code = $@"

            namespace {model.Symbol.ContainingNamespace}.{Constants.CodeGenNamespace}
            {{
                [{library.GeneratedCodeAttribute}(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [{library.GenerateSerializerAttribute}]
                [{library.UseActivatorAttribute}]
                internal class {typeName}: {library.DeltaRowGroup1.Construct(model.Symbol)}
                {{
                    [{library.IdAttribute}(1)]
                    private readonly SetsHolder _sets = new();

                    [{library.IdAttribute}(2)]
                    private readonly StatsHolder _stats = new();

                    public {typeName}(
                        {library.Int32} id,
                        {library.Int32} capacity,
                        {library.Serializer1.Construct(model.Symbol)} serializer,
                        {library.SerializerSessionPool} sessions)
                        : base(id, capacity, serializer, sessions)
                    {{
                        {model.Properties.Render(p => @$"_stats.{p.Name}Stats.Name = ""{p.Name}"";")}
                    }}

                    protected override void OnAdded({model.Symbol} row)
                    {{
                        {model.Properties.Render(p => $@"
                        if (_sets.{p.Name}Set.Add(row.{p.Name}))
                        {{
                            _stats.{p.Name}Stats.DistinctValueCount = _sets.{p.Name}Set.Count;
                        }}

                        if (row.{p.Name} == default)
                        {{
                            _stats.{p.Name}Stats.DefaultValueCount++;
                        }}

                        _stats.{p.Name}Stats.RowCount++;
                        ")}
                    }}

                    protected override void OnBuildStats({library.RowGroupStatsBuilder} builder)
                    {{
                        {model.Properties.Render(p => $@"builder.ColumnSegmentStats[""{p.Name}""] = _stats.{p.Name}Stats.ToImmutable();")}
                    }}

                    [{library.GenerateSerializerAttribute}]
                    internal class SetsHolder
                    {{
                        {model.Properties.Render((p, i) => @$"
                        [{library.IdAttribute}({model.PropertyIds[i]})]
                        public {library.HashSet.Construct(p.Type)} {p.Name}Set = new();")}
                    }}

                    [{library.GenerateSerializerAttribute}]
                    internal class StatsHolder
                    {{
                        {model.Properties.Render((p, i) => @$"
                        [{library.IdAttribute}({model.PropertyIds[i]})]
                        public {library.ColumnSegmentStatsBuilder} {p.Name}Stats = {library.ColumnSegmentStats}.CreateBuilder();")}
                    }}
                }}

                [{library.GeneratedCodeAttribute}(""{nameof(DeltaRowGroupGenerator)}"", ""{Assembly.GetExecutingAssembly().GetName().Version}"")]
                [{library.RegisterDeltaRowFactoryAttribute}(typeof({model.Symbol}))]
                internal class {typeName}Factory: {library.DeltaRowGroupFactory1.Construct(model.Symbol)}
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
                    {library.IActivator1.ToDisplayString().Replace("<T>", $"<{library.DeltaRowGroup1.Construct(model.Symbol)}>")}
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

                    {library.DeltaRowGroup1.Construct(model.Symbol)} {library.IActivator1.ToDisplayString().Replace("<T>", $"<{library.DeltaRowGroup1.Construct(model.Symbol)}>")}.Create() => Create();
                }}
            }}
        ";

        var tree = CSharpSyntaxTree.ParseText(code);
        var text = SourceText.From(tree.GetRoot().NormalizeWhitespace().ToFullString(), Encoding.UTF8);
        var name = $"{compilation.AssemblyName}.{Constants.CodeGenNamespace}.{typeName}.g.cs";

        return new SourceResult(tree, text, name);
    }
}