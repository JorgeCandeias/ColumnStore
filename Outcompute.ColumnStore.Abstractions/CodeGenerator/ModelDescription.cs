using System.Reflection;

namespace Outcompute.ColumnStore.CodeGenerator;

/// <summary>
/// Describes the model to generate code for.
/// </summary>
internal record ModelDescription(
    string Name,
    string FullName,
    bool IsValueType,
    Assembly Assembly,
    IReadOnlyList<PropertyInfo> Properties);