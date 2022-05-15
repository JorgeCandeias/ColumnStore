using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Outcompute.ColumnStore;

internal static class CompressedRowGroupFactory
{
    private static readonly ConcurrentDictionary<Type, Type> _lookup = new();

    public static CompressedRowGroup<TRow> Create<TRow>()
    {
        var type = GetOrAddType<TRow>();

        return (CompressedRowGroup<TRow>)Activator.CreateInstance(type);
    }

    public static void ReadyType<TRow>()
    {
        GetOrAddType<TRow>();
    }

    private static Type GetOrAddType<TRow>() => _lookup.GetOrAdd(typeof(TRow), k => CreateType(k));

    [SuppressMessage("Major Code Smell", "S3011:Reflection should not be used to increase accessibility of classes, methods, or fields", Justification = "Generated Code")]
    private static Type CreateType(Type model)
    {
        // we need to reflect the type to inspect its properties emit code
        // this is a one-off cost per type so performance is not critical

        // ensure the type is marked
        if (!model.IsDefined(typeof(ColumnStoreAttribute)))
        {
            ThrowHelper.ThrowInvalidOperationException($"Type '{model.Name}' is not marked with '{nameof(ColumnStoreAttribute)}'");
        }

        // get all the marked properties
        var props = model.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(x => x.IsDefined(typeof(ColumnStorePropertyAttribute))).ToList();
        if (props.Count <= 0)
        {
            ThrowHelper.ThrowInvalidOperationException($"Type '{model.Name}' does not have any properties marked with '{nameof(ColumnStorePropertyAttribute)}'");
        }

        var typeNamespace = "Outcompute.ColumnStore.GeneratedCode";
        var typeName = $"{model.FullName.Replace(".", "")}CompressedRowGroup";

        var code = $@"

            using System;
            using System.Collections.Generic;
            using Outcompute.ColumnStore;
            using CommunityToolkit.Diagnostics;
            using System.Runtime.CompilerServices;

            namespace {typeNamespace}
            {{
                internal class {typeName}: CompressedRowGroup<{model.FullName}>
                {{
                    {props.Aggregate("", (txt, p) => @$"{txt}
                    private readonly ColumnSegment<{p.PropertyType.FullName}> _{p.Name}Segment = new(""{p.Name}"");")}

                    public override void Add({model.FullName} row)
                    {{
                        {(model.IsValueType ? "" : "Guard.IsNotNull(row, nameof(row));")}

                        {props.Aggregate("", (txt, p) => @$"{txt}
                        _{p.Name}Segment.Add(row.{p.Name});")}

                        Count++;
                    }}

                    public override IEnumerator<{model.FullName}> GetEnumerator()
                    {{
                        {props.Aggregate("", (txt, p) => @$"{txt}
                        var {p.Name}Enumerator = _{p.Name}Segment.EnumerateRows().GetEnumerator();")}

                        while ({props.Aggregate("", (txt, p) => $"{txt}{(txt.Length > 0 ? " && " : "")}{p.Name}Enumerator.MoveNext()")})
                        {{
                            yield return new {model.FullName}
                            {{
                                {props.Aggregate("", (txt, p) => @$"{txt}
                                {p.Name} = {p.Name}Enumerator.Current,")}
                            }};
                        }}
                    }}

                    public override RowGroupStats GetStats()
                    {{
                        var stats = new List<ColumnSegmentStats>();

                        {props.Aggregate("", (txt, p) => @$"{txt}
                        stats.Add(_{p.Name}Segment.GetStats());")}

                        return new RowGroupStats(Count, stats);
                    }}
                }}
            }}
        ";

        var syntax = CSharpSyntaxTree.ParseText(code);

        var references = new MetadataReference[]
        {
            MetadataReference.CreateFromFile(Assembly.Load("netstandard").Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(CompressedRowGroup<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Guard).Assembly.Location),
            MetadataReference.CreateFromFile(model.Assembly.Location)
        };

        var options = new CSharpCompilationOptions(
            OutputKind.DynamicallyLinkedLibrary,
            optimizationLevel: OptimizationLevel.Release);

        var compilation = CSharpCompilation.Create(typeNamespace, new[] { syntax }, references, options);

        using var stream = new MemoryStream();
        var result = compilation.Emit(stream);

        if (!result.Success)
        {
            var error = result.Diagnostics.Where(x => x.IsWarningAsError || x.Severity == DiagnosticSeverity.Error).FirstOrDefault();

            ThrowHelper.ThrowInvalidOperationException($"Could not generate assembly for type '{model.Name}': {error?.GetMessage()}");
        }

        stream.Seek(0, SeekOrigin.Begin);

        var context = new DynamicAssemblyLoadContext();
        var assembly = context.LoadFromStream(stream);

        //var assembly = Assembly.Load(stream.ToArray());

        return assembly.GetType($"{typeNamespace}.{typeName}");
    }
}