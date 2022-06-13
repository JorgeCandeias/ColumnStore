using Microsoft.Extensions.DependencyInjection;

namespace Outcompute.ColumnStore;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddColumnStore(this IServiceCollection services)
    {
        Guard.IsNotNull(services, nameof(services));

        return services
            .AddOptions()
            .AddGeneratedFactories()
            .AddSingleton(typeof(IColumnStoreFactory<>), typeof(ColumnStoreFactory<>))
            .AddSingleton(typeof(DeltaStoreFactory<>))
            .AddSingleton(typeof(IColumnSegmentBuilderFactory<>), typeof(ColumnSegmentBuilderFactory<>))
            .AddTransient(typeof(ColumnSegmentBuilder<>));
    }

    private static IServiceCollection AddGeneratedFactories(this IServiceCollection services)
    {
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            foreach (var implementation in assembly.GetTypes())
            {
                if (implementation.IsDefined(typeof(RegisterDeltaRowFactoryAttribute), false))
                {
                    var attribute = (RegisterDeltaRowFactoryAttribute)implementation.GetCustomAttributes(typeof(RegisterDeltaRowFactoryAttribute), false)[0];

                    // add the service on its own
                    services.AddSingleton(implementation);

                    // add the service against the base class
                    services.AddSingleton(typeof(DeltaRowGroupFactory<>).MakeGenericType(attribute.ModelType), sp => sp.GetRequiredService(implementation));
                }

                // todo
            }
        }

        return services;
    }
}