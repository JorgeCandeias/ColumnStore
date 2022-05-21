namespace Outcompute.ColumnStore;

/// <summary>
/// Marks a property to be included in the associated column store.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class ColumnStorePropertyAttribute : Attribute
{
    /// <inheritdoc cref="ColumnStorePropertyAttribute"/>
    /// <param name="comparer">
    /// Defines a custom comparer type for the annotated property in the context of the column store.
    /// The comparer type must derive from <see cref="IComparer{T}"/> and must have an instance constructor without parameters.
    /// The comparer type can be an open generic type. If so, a closed generic type will be created using the type of the property.
    /// </param>
    public ColumnStorePropertyAttribute(Type? comparer = null)
    {
        Comparer = comparer;
    }

    /// <summary>
    /// The optional comparer to use in the context of the column store.
    /// </summary>
    public Type? Comparer { get; }
}