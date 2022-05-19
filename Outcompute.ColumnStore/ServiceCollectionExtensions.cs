using Microsoft.Extensions.DependencyInjection;
using Outcompute.ColumnStore.CodeGenerator;

namespace Outcompute.ColumnStore;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddColumnStore(this IServiceCollection services)
    {
        Guard.IsNotNull(services, nameof(services));

        return services
            .AddTransient<IModelDescriber, ModelDescriber>()
            .AddSingleton(typeof(IDeltaRowGroupFactory<>), typeof(DeltaRowGroupFactory<>));
    }
}