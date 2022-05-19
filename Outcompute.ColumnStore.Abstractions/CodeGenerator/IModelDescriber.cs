namespace Outcompute.ColumnStore.CodeGenerator;

internal interface IModelDescriber
{
    public ModelDescription Describe(Type model);
}