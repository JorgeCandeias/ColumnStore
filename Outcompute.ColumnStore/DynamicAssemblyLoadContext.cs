using System.Reflection;
using System.Runtime.Loader;

namespace Outcompute.ColumnStore;

/// <summary>
/// A dummy assembly load context to load generated code into.
/// </summary>
internal class DynamicAssemblyLoadContext : AssemblyLoadContext
{
    protected override Assembly Load(AssemblyName assemblyName)
    {
        return null!;
    }
}