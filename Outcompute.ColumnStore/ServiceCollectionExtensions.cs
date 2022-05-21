using Microsoft.Extensions.DependencyInjection;

namespace Outcompute.ColumnStore;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddColumnStore(this IServiceCollection services)
    {
        Guard.IsNotNull(services, nameof(services));

        return services
            .AddOptions()
            .AddSingleton(typeof(IDeltaRowGroupFactory<>), typeof(DeltaRowGroupFactory<>))
            .AddTransient(typeof(IDeltaStore<>), typeof(DeltaStore<>));
    }
}