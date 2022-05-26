using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using System.Reflection;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class OrleansSerializationSourceDriver
{
    public static IEnumerable<GeneratedSourceResult> Generate(Compilation compilation, IEnumerable<SourceText> sources)
    {
        // remove all original syntax trees so we dont generate duplicate code on the user project
        compilation = compilation.RemoveAllSyntaxTrees();

        // add all specified sources to the skeleton compilation
        foreach (var source in sources)
        {
            var tree = CSharpSyntaxTree.ParseText(source);
            compilation = compilation.AddSyntaxTrees(tree);
        }

        // hack to access the orleans source generator since the nuget package is a dev dependency
        var assembly = Assembly.Load("Orleans.CodeGenerator");
        var type = assembly.GetType("Orleans.CodeGenerator.OrleansSerializationSourceGenerator", true);
        var generator = (ISourceGenerator)Activator.CreateInstance(type);

        // run the orleans serialization source generator on the target code
        var result = CSharpGeneratorDriver
            .Create(generator)
            .RunGenerators(compilation)
            .GetRunResult();

        // yield the generated source code
        foreach (var item in result.Results)
        {
            foreach (var source in item.GeneratedSources)
            {
                yield return source;
            }
        }
    }
}