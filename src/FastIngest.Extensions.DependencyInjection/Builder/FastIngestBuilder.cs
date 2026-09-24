using System.Reflection;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using Microsoft.Extensions.DependencyInjection;

namespace FastIngest.Extensions.DependencyInjection.Builder;

/// <summary>
/// Default builder implementation for configuring FastIngest dependencies and profile registrations.
/// </summary>
public class FastIngestBuilder : IFastIngestBuilder
{
    private readonly IFastIngestProfileRegistry _registry;

    /// <inheritdoc/>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="FastIngestBuilder"/> class.
    /// </summary>
    /// <param name="services">The DI service collection.</param>
    /// <param name="registry">The singleton profile registry.</param>
    public FastIngestBuilder(IServiceCollection services, IFastIngestProfileRegistry registry)
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddPostgreSqlSink(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        Services.Configure<FastIngestOptions>(options =>
        {
            options.DefaultConnectionString = connectionString;
        });

        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddSqlServerSink(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        Services.Configure<FastIngestOptions>(options =>
        {
            options.DefaultConnectionString = connectionString;
        });

        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddMongoDbSink()
    {
        FastIngestServiceExtensions.AddMongoDbSink(this);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddMongoDbSink(string connectionString, string? databaseName = null)
    {
        FastIngestServiceExtensions.AddMongoDbSink(this, connectionString, databaseName);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddMySqlSink()
    {
        FastIngestServiceExtensions.AddMySqlSink(this);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddMySqlSink(string connectionString)
    {
        FastIngestServiceExtensions.AddMySqlSink(this, connectionString);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddSqliteSink()
    {
        FastIngestServiceExtensions.AddSqliteSink(this);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddSqliteSink(string connectionString)
    {
        FastIngestServiceExtensions.AddSqliteSink(this, connectionString);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddCosmosDbSink()
    {
        FastIngestServiceExtensions.AddCosmosDbSink(this);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddCosmosDbSink(string connectionString, string? databaseName = null, string? containerName = null)
    {
        FastIngestServiceExtensions.AddCosmosDbSink(this, connectionString, databaseName, containerName);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddCosmosDbSink(Microsoft.Azure.Cosmos.CosmosClient cosmosClient, string? databaseName = null, string? containerName = null)
    {
        FastIngestServiceExtensions.AddCosmosDbSink(this, cosmosClient, databaseName, containerName);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddCosmosDbSink(Microsoft.Azure.Cosmos.Container container)
    {
        FastIngestServiceExtensions.AddCosmosDbSink(this, container);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddElasticsearchSink()
    {
        FastIngestServiceExtensions.AddElasticsearchSink(this);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddElasticsearchSink(Elastic.Clients.Elasticsearch.ElasticsearchClient client)
    {
        FastIngestServiceExtensions.AddElasticsearchSink(this, client);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddElasticsearchSink(Action<Elastic.Clients.Elasticsearch.ElasticsearchClientSettings> configureSettings)
    {
        FastIngestServiceExtensions.AddElasticsearchSink(this, configureSettings);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddElasticsearchSink(Uri endpoint, string? apiKey = null, string? defaultIndex = null)
    {
        FastIngestServiceExtensions.AddElasticsearchSink(this, endpoint, apiKey, defaultIndex);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder AddElasticsearchSink(string endpoint, string? apiKey = null, string? defaultIndex = null)
    {
        FastIngestServiceExtensions.AddElasticsearchSink(this, endpoint, apiKey, defaultIndex);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder RegisterProfilesFromAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);

        var profileTypes = assembly.GetTypes()
            .Where(t => !t.IsAbstract && !t.IsInterface && typeof(IFastIngestProfile).IsAssignableFrom(t));

        foreach (var type in profileTypes)
        {
            if (type.GetConstructor(Type.EmptyTypes) != null)
            {
                RegisterProfile(type);
            }
        }

        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder RegisterProfilesFromAssemblies(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        foreach (var assembly in assemblies)
        {
            RegisterProfilesFromAssembly(assembly);
        }

        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder RegisterProfile<TProfile>() where TProfile : class, IFastIngestProfile, new()
    {
        var instance = new TProfile();
        RegisterProfileInstance(instance);
        return this;
    }

    /// <inheritdoc/>
    public IFastIngestBuilder RegisterProfile(Type profileType)
    {
        ArgumentNullException.ThrowIfNull(profileType);

        if (!typeof(IFastIngestProfile).IsAssignableFrom(profileType) || profileType.IsAbstract)
        {
            throw new ArgumentException($"Type '{profileType.FullName}' must implement IFastIngestProfile and not be abstract.", nameof(profileType));
        }

        var instance = (IFastIngestProfile)Activator.CreateInstance(profileType)!;
        RegisterProfileInstance(instance);
        return this;
    }

    private void RegisterProfileInstance(IFastIngestProfile profile)
    {
        _registry.Register(profile);

        // Register profile instance in DI container as singleton
        Services.AddSingleton(profile.GetType(), profile);
        Services.AddSingleton(typeof(IFastIngestProfile), profile);

        var profileInterface = profile.GetType().GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IFastIngestProfile<>));

        if (profileInterface != null)
        {
            Services.AddSingleton(profileInterface, profile);
        }
    }
}
