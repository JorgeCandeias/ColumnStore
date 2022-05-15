using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Collections.Concurrent;
using System.Reflection;

namespace Outcompute.ColumnStore;

internal static class CompressedRowGroupFactory
{
    private static readonly ConcurrentDictionary<Type, Type> _lookup = new();

    public static CompressedRowGroup<TRow> Create<TRow>()
    {
        var type = _lookup.GetOrAdd(typeof(TRow), k => CreateType(k));

        return (CompressedRowGroup<TRow>)Activator.CreateInstance(type);
    }

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
        var typeName = $"{model.FullName.Replace(".", "")}CompressRowGroup";

        var code = $@"

            using System;
            using System.Collections.Generic;
            using Outcompute.ColumnStore;
            using CommunityToolkit.Diagnostics;

            namespace {typeNamespace}
            {{
                public class {typeName}: CompressedRowGroup<{model.FullName}>
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

                        while ({props.Aggregate("", (txt, p) => $"{txt}{(txt.Length > 0 ? " && ": "")}{p.Name}Enumerator.MoveNext()")})
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

        var assemblyName = Path.GetRandomFileName(); // todo: use target class name

        var references = new MetadataReference[]
        {
            MetadataReference.CreateFromFile(Assembly.Load("netstandard").Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(CompressedRowGroup<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Guard).Assembly.Location),
            MetadataReference.CreateFromFile(model.Assembly.Location)
        };

        var compilation = CSharpCompilation.Create(
            assemblyName,
            new[] { syntax },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, optimizationLevel: OptimizationLevel.Release));

        Assembly assembly;
        using (var stream = new MemoryStream())
        {
            var result = compilation.Emit(stream);

            if (!result.Success)
            {
                var error = result.Diagnostics.Where(x => x.IsWarningAsError || x.Severity == DiagnosticSeverity.Error).FirstOrDefault();

                ThrowHelper.ThrowInvalidOperationException($"Could not generate assembly for type '{model.Name}': {error?.GetMessage()}");
            }

            stream.Seek(0, SeekOrigin.Begin);
            assembly = Assembly.Load(stream.ToArray());
        }

        return assembly.GetType($"{typeNamespace}.{typeName}");
    }
}