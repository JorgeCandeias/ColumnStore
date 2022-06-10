using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace Outcompute.ColumnStore.CodeGenerator
{
    [Generator]
    internal class SourceGenerator : ISourceGenerator
    {
        public void Initialize(GeneratorInitializationContext context)
        {
            context.RegisterForSyntaxNotifications(() => new ColumnStoreSyntaxContextReceiver());
        }

        public void Execute(GeneratorExecutionContext context)
        {
            if (context.SyntaxContextReceiver is not ColumnStoreSyntaxContextReceiver receiver)
            {
                return;
            }

            var assemblyName = context.Compilation.AssemblyName ?? "Assembly";
            var libs = new LibraryTypes(context.Compilation);

            var result = ColumnStoreSourceDriver.Generate(context.Compilation, receiver.Model, libs);
            var text = result.NormalizeWhitespace().ToFullString();

            context.AddSource($"{assemblyName}.ColumnStore.g.cs", text);

            // hack to de-duplicate the orleans assembly metadata class name
            var search = $"Metadata_{context.Compilation.AssemblyName!.Replace('.', '_')}";
            var replace = $"Metadata_ColumnStoreCodeGen_{context.Compilation.AssemblyName!.Replace('.', '_')}";
            var changes = new List<TextChange>();

            foreach (var item in OrleansSerializationSourceDriver.Generate(context.Compilation, new[] { SourceText.From(text) }))
            {
                var source = item.SourceText;
                foreach (var line in source.Lines)
                {
                    var index = line.ToString().IndexOf(search);
                    if (index >= 0)
                    {
                        changes.Add(new TextChange(new TextSpan(line.Start + index, search.Length), replace));
                    }
                }

                if (changes.Count > 0)
                {
                    source = source.WithChanges(changes);
                }

                context.AddSource($"{context.Compilation.AssemblyName}.ColumnStore.{item.HintName}", source);
            }
        }
    }
}