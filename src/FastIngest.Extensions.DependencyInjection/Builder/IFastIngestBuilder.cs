using System.Reflection;
using FastIngest.Extensions.DependencyInjection.Profiles;
using Microsoft.Extensions.DependencyInjection;

namespace FastIngest.Extensions.DependencyInjection.Builder;

/// <summary>
/// Builder interface for configuring FastIngest services, sinks, and profiles.
/// </summary>
public interface IFastIngestBuilder
{
    /// <summary>
    /// Gets the application service collection.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Configures the default PostgreSQL connection string for the FastIngest engine.
    /// </summary>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddPostgreSqlSink(string connectionString);

    /// <summary>
    /// Configures the default Microsoft SQL Server connection string for the FastIngest engine.
    /// </summary>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddSqlServerSink(string connectionString);

    /// <summary>
    /// Configures MongoDB bulk write sink capabilities for the FastIngest engine.
    /// </summary>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddMongoDbSink();

    /// <summary>
    /// Configures default MongoDB connection settings for the FastIngest engine.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddMongoDbSink(string connectionString, string? databaseName = null);

    /// <summary>
    /// Discovers and registers all concrete <see cref="IFastIngestProfile"/> classes in the specified assembly.
    /// </summary>
    /// <param name="assembly">The assembly to scan for profiles.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder RegisterProfilesFromAssembly(Assembly assembly);

    /// <summary>
    /// Discovers and registers all concrete <see cref="IFastIngestProfile"/> classes across multiple assemblies.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan for profiles.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder RegisterProfilesFromAssemblies(params Assembly[] assemblies);

    /// <summary>
    /// Registers a specific profile type and pre-compiles its mappings.
    /// </summary>
    /// <typeparam name="TProfile">The concrete profile type.</typeparam>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder RegisterProfile<TProfile>() where TProfile : class, IFastIngestProfile, new();

    /// <summary>
    /// Registers a specific profile type and pre-compiles its mappings.
    /// </summary>
    /// <param name="profileType">The concrete profile type implementing <see cref="IFastIngestProfile"/>.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder RegisterProfile(Type profileType);
}
