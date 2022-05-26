using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class OrleansSerializationSourceDriver
{
    public static void Generate(Compilation compilation, IEnumerable<SourceText> sources)
    {
        // remove all original syntax trees so we dont generate duplicate code on the user project
        compilation = compilation.RemoveAllSyntaxTrees();

        // add all specified sources as syntax treets
        foreach (var source in sources)
        {
            var tree = CSharpSyntaxTree.ParseText(source);
            compilation = compilation.AddSyntaxTrees(tree);
        }

        Type type = null!;
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            type = assembly.GetType("Orleans.CodeGenerator.OrleansSerializationSourceGenerator", false);
        }

        if (type is null)
        {
            throw new DllNotFoundException();
        }

        /*
        var driver = CSharpGeneratorDriver.Create(new Orleans.CodeGenerator.OrleansSerializationSourceGenerator());
        driver = (CSharpGeneratorDriver)driver.RunGenerators(compilation);
        var result = driver.GetRunResult();

        foreach (var item in result.Results)
        {
            foreach (var source in item.GeneratedSources)
            {
                //context.AddSource(source.HintName, source.SourceText);
            }
        }
        */
    }
}