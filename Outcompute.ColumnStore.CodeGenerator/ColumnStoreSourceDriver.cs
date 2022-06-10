using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class ColumnStoreSourceDriver
{
    public static CompilationUnitSyntax Generate(Compilation compilation, SourceModel model, LibraryTypes library)
    {
        var unit = CompilationUnit();

        // generate the using directives for the code file
        unit = unit.AddUsings(UsingsGenerator.Generate());

        // run code generation for each user model
        foreach (var item in model.ColumnStoreTypes)
        {
            // create a flat model description to make code generation easier
            var descriptor = new ColumnStoreTypeDescription
            {
                GeneratedNamespace = $"{item.ContainingNamespace.ToDisplayString()}.ColumnStoreCodeGen",
                Symbol = item,
            };

            foreach (var property in item.GetMembers().OfType<IPropertySymbol>().Where(x => x.GetAttributes().Any(x => x.AttributeClass?.Equals(library.ColumnStorePropertyAttribute, SymbolEqualityComparer.Default) ?? false)))
            {
                descriptor.Properties.Add(property);
            }

            // generate code for this model
            unit = unit
                //.AddMembers(DeltaRowGroupGenerator.Generate(descriptor, library))
                .AddMembers(SolidRowGroupGenerator.Generate(descriptor, library))
                .AddMembers(SolidRowGroupFactoryGenerator.Generate(descriptor, library));
        }

        return unit;
    }
}