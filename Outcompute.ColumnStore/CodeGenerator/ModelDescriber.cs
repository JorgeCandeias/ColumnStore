using System.Reflection;

namespace Outcompute.ColumnStore.CodeGenerator;

internal class ModelDescriber : IModelDescriber
{
    public ModelDescription Describe(Type model)
    {
        Guard.IsNotNull(model, nameof(model));

        // ensure the type is marked
        if (!model.IsDefined(typeof(ColumnStoreAttribute)))
        {
            ThrowHelper.ThrowInvalidOperationException($"Type '{model.Name}' is not marked with '{nameof(ColumnStoreAttribute)}'");
        }

        // get all the marked properties
        var props = model
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(x => x.IsDefined(typeof(ColumnStorePropertyAttribute)))
            .ToList();

        if (props.Count <= 0)
        {
            ThrowHelper.ThrowInvalidOperationException($"Type '{model.Name}' does not have any properties marked with '{nameof(ColumnStorePropertyAttribute)}'");
        }

        return new ModelDescription(model.Name, model.FullName, model.IsValueType, model.Assembly, props);
    }
}