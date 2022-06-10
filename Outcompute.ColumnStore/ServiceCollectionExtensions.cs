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
            .AddSingleton(typeof(IDeltaStoreFactory<>), typeof(DeltaStoreFactory<>))
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
                    services.AddSingleton(implementation);

                    var service = implementation.GetInterface(typeof(IDeltaRowGroupFactory<>).Name);
                    if (service is not null)
                    {
                        services.AddSingleton(service, sp => sp.GetRequiredService(implementation));
                    }
                }

                // todo
            }
        }

        return services;
    }
}