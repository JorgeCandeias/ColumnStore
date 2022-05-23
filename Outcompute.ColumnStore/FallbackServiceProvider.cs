using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Outcompute.ColumnStore;

/// <summary>
/// This service provider steps in when the user creates <see cref="ColumnStore{TRow}"/> instances in-place, without using their own service provider.
/// It contains only the services necessary for the <see cref="Outcompute.ColumnStore"/> instance to work.
/// </summary>
internal static class FallbackServiceProvider
{
    public static IServiceProvider Default { get; } = new ServiceCollection()
        .AddColumnStore()
        .AddSerializer()
        .BuildServiceProvider();
}