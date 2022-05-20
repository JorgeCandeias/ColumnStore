﻿using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Outcompute.ColumnStore.CodeGenerator;

internal static class CodeGenerator
{
    public static CompilationUnitSyntax Generate(Compilation compilation, SourceModel model, LibraryTypes library)
    {
        var unit = CompilationUnit();

        // generate the using directives for the code file
        unit = unit.AddUsings(UsingsGenerator.Generate());

        // generate assembly attributes
        unit = unit.AddAttributeLists(IgnoreAccessChecksToAttributeGenerator.GenerateAttributes());

        // generate the internal access attribute
        unit = unit.AddMembers(IgnoreAccessChecksToAttributeGenerator.GenerateMembers());

        // run code generation for each user model
        foreach (var item in model.ColumnStoreTypes)
        {
            // create a flat model description to make code generation easier
            var descriptor = new ColumnStoreTypeDescription
            {
                GeneratedNamespace = $"{item.ContainingNamespace.ToDisplayString()}.GeneratedCode",
                Symbol = item,
            };

            foreach (var property in item.GetMembers().OfType<IPropertySymbol>().Where(x => x.GetAttributes().Any(x => x.AttributeClass?.Equals(library.ColumnStorePropertyAttribute, SymbolEqualityComparer.Default) ?? false)))
            {
                descriptor.Properties.Add(property);
            }

            // generate the delta row for this model
            unit = unit.AddMembers(DeltaRowGroupGenerator.Generate(descriptor, library));
        }

        return unit;
    }
}