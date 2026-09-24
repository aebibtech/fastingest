using System.Reflection;
using FastIngest.Extensions.DependencyInjection.Builder;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FastIngest.Extensions.DependencyInjection;

/// <summary>
/// Service collection extensions for configuring FastIngest dependency injection.
/// </summary>
public static class FastIngestServiceExtensions
{
    /// <summary>
    /// Registers FastIngest core services, singleton profile cache, and scoped engine.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configureOptions">Optional configuration action for <see cref="FastIngestOptions"/>.</param>
    /// <returns>A configured <see cref="IFastIngestBuilder"/> instance.</returns>
    public static IFastIngestBuilder AddFastIngest(
        this IServiceCollection services,
        Action<FastIngestOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions();
        if (configureOptions != null)
        {
            services.Configure(configureOptions);
        }

        // Ensure singleton profile registry is registered
        services.TryAddSingleton<FastIngestProfileRegistry>();
        services.TryAddSingleton<IFastIngestProfileRegistry>(sp => sp.GetRequiredService<FastIngestProfileRegistry>());

        // Register scoped engine
        services.TryAddScoped<IFastIngestEngine, FastIngestEngine>();

        // Obtain or create registry for the builder
        var existingDescriptor = services.FirstOrDefault(s => s.ServiceType == typeof(FastIngestProfileRegistry));
        FastIngestProfileRegistry registry;
        if (existingDescriptor?.ImplementationInstance is FastIngestProfileRegistry inst)
        {
            registry = inst;
        }
        else
        {
            registry = new FastIngestProfileRegistry();
            // Replace descriptor with instance so builder and DI share the exact same instance
            services.RemoveAll<FastIngestProfileRegistry>();
            services.RemoveAll<IFastIngestProfileRegistry>();
            services.AddSingleton(registry);
            services.AddSingleton<IFastIngestProfileRegistry>(registry);
        }

        return new FastIngestBuilder(services, registry);
    }

    /// <summary>
    /// Registers FastIngest core services and configures profiles and sinks using a builder callback.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configure">Configuration delegate for <see cref="IFastIngestBuilder"/>.</param>
    /// <returns>The configured <see cref="IFastIngestBuilder"/>.</returns>
    public static IFastIngestBuilder AddFastIngest(
        this IServiceCollection services,
        Action<IFastIngestBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = services.AddFastIngest();
        configure(builder);
        return builder;
    }

    /// <summary>
    /// Configures Microsoft SQL Server bulk copy sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddSqlServerSink(this FastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder;
    }

    /// <summary>
    /// Configures Microsoft SQL Server bulk copy sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddSqlServerSink(this IFastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder;
    }

    /// <summary>
    /// Configures the default Microsoft SQL Server connection string for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddSqlServerSink(
        this FastIngestBuilder builder,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.DefaultConnectionString = connectionString;
        });

        return builder;
    }

    /// <summary>
    /// Configures the default Microsoft SQL Server connection string for FastIngest.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlServerSink(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.Configure<FastIngestOptions>(options =>
        {
            options.DefaultConnectionString = connectionString;
        });

        return services;
    }

    /// <summary>
    /// Configures the default PostgreSQL connection string for FastIngest.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddPostgreSqlSink(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.Configure<FastIngestOptions>(options =>
        {
            options.DefaultConnectionString = connectionString;
        });

        return services;
    }

    /// <summary>
    /// Scans an assembly and registers all discovered FastIngest profiles.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="assembly">The assembly to scan.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection RegisterProfilesFromAssembly(
        this IServiceCollection services,
        Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(assembly);

        var builder = services.AddFastIngest();
        builder.RegisterProfilesFromAssembly(assembly);
        return services;
    }
}
