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
            var library = new LibraryTypes(context.Compilation);

            // generate sources for each model
            var sources = new List<SourceText>();
            foreach (var item in receiver.Model.ColumnStoreTypes)
            {
                // create a flat model description to make code generation easier
                var descriptor = new Model
                {
                    GeneratedNamespace = $"{item.ContainingNamespace.ToDisplayString()}.ColumnStoreCodeGen",
                    Symbol = item,
                };

                // add properties
                foreach (var property in item.GetMembers().OfType<IPropertySymbol>())
                {
                    var idAttribute = property.GetAttributes().SingleOrDefault(x => x.AttributeClass?.Equals(library.IdAttribute, SymbolEqualityComparer.Default) ?? false);
                    if (idAttribute is not null)
                    {
                        descriptor.Properties.Add(property);
                        descriptor.PropertyIds.Add((ushort)idAttribute.ConstructorArguments[0].Value!);
                    }
                }

                // generate delta row code
                var generated = DeltaRowGroupGenerator.Generate(context.Compilation, descriptor, library);
                sources.Add(generated.Text);
                context.AddSource(generated.Name, generated.Text);
            }

            // toremove

            var result = ColumnStoreSourceDriver.Generate(context.Compilation, receiver.Model, library);
            var text = result.NormalizeWhitespace().ToFullString();

            context.AddSource($"{assemblyName}.ColumnStore.g.cs", text);

            // hack to de-duplicate the orleans assembly metadata class name
            var search = $"Metadata_{context.Compilation.AssemblyName!.Replace('.', '_')}";
            var replace = $"Metadata_ColumnStoreCodeGen_{context.Compilation.AssemblyName!.Replace('.', '_')}";
            var changes = new List<TextChange>();

            foreach (var item in OrleansSerializationSourceDriver.Generate(context.Compilation, sources.Append(SourceText.From(text)).ToArray()))
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