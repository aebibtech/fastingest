using FastIngest.AspNetCore.Workers;
using FastIngest.Extensions.DependencyInjection;
using FastIngest.Extensions.DependencyInjection.Builder;
using FastIngest.Extensions.DependencyInjection.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FastIngest.AspNetCore.Extensions;

/// <summary>
/// Service collection extension methods for registering FastIngest ASP.NET Core integration,
/// SignalR real-time hubs, and background worker infrastructure.
/// </summary>
public static class FastIngestAspNetCoreServiceExtensions
{
    /// <summary>
    /// Registers FastIngest core services, background job worker, SignalR hub, and engine options.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configureOptions">Optional configuration action for <see cref="FastIngestOptions"/>.</param>
    /// <returns>The configured <see cref="IFastIngestBuilder"/> instance.</returns>
    public static IFastIngestBuilder AddFastIngestAspNetCore(
        this IServiceCollection services,
        Action<FastIngestOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSignalR();
        services.TryAddSingleton<IngestJobQueue>();
        services.AddHostedService<IngestBackgroundService>();

        return services.AddFastIngest(configureOptions);
    }

    /// <summary>
    /// Registers FastIngest core services, background worker, SignalR hub, and configures sinks using a builder callback.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configure">Configuration delegate for <see cref="IFastIngestBuilder"/>.</param>
    /// <returns>The configured <see cref="IFastIngestBuilder"/> instance.</returns>
    public static IFastIngestBuilder AddFastIngestAspNetCore(
        this IServiceCollection services,
        Action<IFastIngestBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = services.AddFastIngestAspNetCore();
        configure(builder);
        return builder;
    }
}
