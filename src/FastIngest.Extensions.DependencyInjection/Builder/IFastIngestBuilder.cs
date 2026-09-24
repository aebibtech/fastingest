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
    /// Configures MySQL bulk sink capabilities for the FastIngest engine.
    /// </summary>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddMySqlSink();

    /// <summary>
    /// Configures the default MySQL connection string for the FastIngest engine.
    /// </summary>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddMySqlSink(string connectionString);

    /// <summary>
    /// Configures SQLite bulk sink capabilities for the FastIngest engine.
    /// </summary>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddSqliteSink();

    /// <summary>
    /// Configures the default SQLite connection string for the FastIngest engine.
    /// </summary>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddSqliteSink(string connectionString);

    /// <summary>
    /// Configures Azure Cosmos DB bulk sink capabilities for the FastIngest engine.
    /// </summary>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddCosmosDbSink();

    /// <summary>
    /// Configures default Azure Cosmos DB connection settings for the FastIngest engine.
    /// </summary>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddCosmosDbSink(string connectionString, string? databaseName = null, string? containerName = null);

    /// <summary>
    /// Registers an existing <see cref="Microsoft.Azure.Cosmos.CosmosClient"/> instance for the FastIngest engine.
    /// </summary>
    /// <param name="cosmosClient">The configured Cosmos DB client.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddCosmosDbSink(Microsoft.Azure.Cosmos.CosmosClient cosmosClient, string? databaseName = null, string? containerName = null);

    /// <summary>
    /// Registers an existing <see cref="Microsoft.Azure.Cosmos.Container"/> instance for the FastIngest engine.
    /// </summary>
    /// <param name="container">The configured Cosmos DB container.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddCosmosDbSink(Microsoft.Azure.Cosmos.Container container);

    /// <summary>
    /// Configures Elasticsearch bulk sink capabilities for the FastIngest engine.
    /// </summary>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddElasticsearchSink();

    /// <summary>
    /// Registers an existing <see cref="Elastic.Clients.Elasticsearch.ElasticsearchClient"/> instance for the FastIngest engine.
    /// </summary>
    /// <param name="client">The configured Elasticsearch client.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddElasticsearchSink(Elastic.Clients.Elasticsearch.ElasticsearchClient client);

    /// <summary>
    /// Configures Elasticsearch client settings using a configuration action.
    /// </summary>
    /// <param name="configureSettings">Action configuring <see cref="Elastic.Clients.Elasticsearch.ElasticsearchClientSettings"/>.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddElasticsearchSink(Action<Elastic.Clients.Elasticsearch.ElasticsearchClientSettings> configureSettings);

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI and optional API key.
    /// </summary>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddElasticsearchSink(Uri endpoint, string? apiKey = null, string? defaultIndex = null);

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI string and optional API key.
    /// </summary>
    /// <param name="endpoint">The Elasticsearch server endpoint URI string.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    IFastIngestBuilder AddElasticsearchSink(string endpoint, string? apiKey = null, string? defaultIndex = null);

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
