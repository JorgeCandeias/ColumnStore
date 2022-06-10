namespace Outcompute.ColumnStore;

[AttributeUsage(AttributeTargets.Class)]
public class RegisterDeltaRowFactoryAttribute : Attribute
{
    public RegisterDeltaRowFactoryAttribute(Type modelType)
    {
        Guard.IsNotNull(modelType, nameof(modelType));

        ModelType = modelType;
    }

    public Type ModelType { get; }
}