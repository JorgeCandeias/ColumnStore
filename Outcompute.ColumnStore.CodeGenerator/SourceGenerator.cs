using Microsoft.CodeAnalysis;
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

            var libs = LibraryTypes.FromCompilation(context.Compilation);

            var result = ColumnStoreSourceDriver.Generate(context.Compilation, receiver.Model, libs);
            var text = result.NormalizeWhitespace().ToFullString();

            context.AddSource($"{context.Compilation.AssemblyName ?? "Assembly"}.ColumnStore.g.cs", text);

            OrleansSerializationSourceDriver.Generate(context.Compilation, new[] { SourceText.From(text) });
        }
    }
}