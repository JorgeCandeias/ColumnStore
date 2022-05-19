using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Outcompute.ColumnStore.CodeGenerator;
using System.Reflection;

namespace Outcompute.ColumnStore.CodeGeneration;

/// <summary>
/// Generates the classes required to support user models.
/// </summary>
internal class ModelCodeGenerator
{
    private readonly IModelDescriber _describer;

    public ModelCodeGenerator(IModelDescriber describer)
    {
        Guard.IsNotNull(describer, nameof(describer));

        _describer = describer;
    }

    public ModelSupportTypes CreateTypes(Type type)
    {
        var model = _describer.Describe(type);

        var typeNamespace = "Outcompute.ColumnStore.GeneratedCode";
        var typeName = $"{model.FullName.Replace(".", "")}CompressedRowGroup";

        var props = model.Properties;

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

            ThrowHelper.ThrowInvalidOperationException($"Could not generate assembly '{typeNamespace}' with type '{typeName}' for type '{model.Name}': {error?.GetMessage()}");
        }

        stream.Seek(0, SeekOrigin.Begin);

        var context = new DynamicAssemblyLoadContext();
        var assembly = context.LoadFromStream(stream);

        return new ModelSupportTypes(assembly.GetType($"{typeNamespace}.{typeName}"));
    }

    //public static ModelCodeGenerator Default { get; } = new();
}

internal class ModelSupportTypes
{
    public ModelSupportTypes(Type compressedRowGroupType)
    {
        CompressedRowGroupType = compressedRowGroupType;
    }

    public Type CompressedRowGroupType { get; }
}