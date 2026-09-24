using System.Reflection;
using FastIngest.Core.Sinks;
using FastIngest.Extensions.DependencyInjection.Builder;
using FastIngest.Extensions.DependencyInjection.Options;
using FastIngest.Extensions.DependencyInjection.Profiles;
using FastIngest.Extensions.DependencyInjection.Sinks;
using FastIngest.MongoDb;
using FastIngest.MySql;
using FastIngest.Sqlite;
using FastIngest.CosmosDb;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using FastIngest.Elasticsearch;
using MongoDB.Driver;
using MySqlConnector;

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
    /// Configures MongoDB bulk write sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddMongoDbSink(this FastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<IMongoClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;
            var connectionString = options.MongoConnectionString ?? options.DefaultConnectionString;
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    "MongoDB connection string is not configured. Configure MongoConnectionString or DefaultConnectionString in FastIngestOptions, or register an IMongoClient in DI.");
            }

            return new MongoClient(connectionString);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(MongoDbIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures MongoDB bulk write sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddMongoDbSink(this IFastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder is FastIngestBuilder concreteBuilder)
        {
            return concreteBuilder.AddMongoDbSink();
        }

        builder.Services.TryAddSingleton<IMongoClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;
            var connectionString = options.MongoConnectionString ?? options.DefaultConnectionString;
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    "MongoDB connection string is not configured. Configure MongoConnectionString or DefaultConnectionString in FastIngestOptions, or register an IMongoClient in DI.");
            }

            return new MongoClient(connectionString);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(MongoDbIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures default MongoDB connection settings for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddMongoDbSink(
        this FastIngestBuilder builder,
        string connectionString,
        string? databaseName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.MongoConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
            if (databaseName != null)
            {
                options.MongoDatabaseName = databaseName;
            }
        });

        AddMongoDbSink(builder);
        return builder;
    }

    /// <summary>
    /// Configures default MongoDB connection settings for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddMongoDbSink(
        this IFastIngestBuilder builder,
        string connectionString,
        string? databaseName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.MongoConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
            if (databaseName != null)
            {
                options.MongoDatabaseName = databaseName;
            }
        });

        return builder.AddMongoDbSink();
    }

    /// <summary>
    /// Registers an existing <see cref="IMongoClient"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="mongoClient">The configured MongoDB client.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddMongoDbSink(
        this FastIngestBuilder builder,
        IMongoClient mongoClient,
        string? databaseName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(mongoClient);

        builder.Services.AddSingleton<IMongoClient>(mongoClient);

        if (databaseName != null)
        {
            builder.Services.Configure<FastIngestOptions>(options =>
            {
                options.MongoDatabaseName = databaseName;
            });
        }

        AddMongoDbSink(builder);
        return builder;
    }

    /// <summary>
    /// Registers an existing <see cref="IMongoClient"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="mongoClient">The configured MongoDB client.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddMongoDbSink(
        this IFastIngestBuilder builder,
        IMongoClient mongoClient,
        string? databaseName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(mongoClient);

        builder.Services.AddSingleton<IMongoClient>(mongoClient);

        if (databaseName != null)
        {
            builder.Services.Configure<FastIngestOptions>(options =>
            {
                options.MongoDatabaseName = databaseName;
            });
        }

        return builder.AddMongoDbSink();
    }

    /// <summary>
    /// Configures default MongoDB connection settings for FastIngest on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMongoDbSink(
        this IServiceCollection services,
        string connectionString,
        string? databaseName = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.Configure<FastIngestOptions>(options =>
        {
            options.MongoConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
            if (databaseName != null)
            {
                options.MongoDatabaseName = databaseName;
            }
        });

        return services;
    }

    /// <summary>
    /// Registers an existing <see cref="IMongoClient"/> instance on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="mongoClient">The configured MongoDB client.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMongoDbSink(
        this IServiceCollection services,
        IMongoClient mongoClient,
        string? databaseName = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(mongoClient);

        services.AddSingleton<IMongoClient>(mongoClient);

        if (databaseName != null)
        {
            services.Configure<FastIngestOptions>(options =>
            {
                options.MongoDatabaseName = databaseName;
            });
        }

        return services;
    }

    /// <summary>
    /// Configures MySQL bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddMySqlSink(this FastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder;
    }

    /// <summary>
    /// Configures MySQL bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddMySqlSink(this IFastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder;
    }

    /// <summary>
    /// Configures the default MySQL connection string for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddMySqlSink(
        this FastIngestBuilder builder,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.MySqlConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
        });

        return builder;
    }

    /// <summary>
    /// Configures the default MySQL connection string for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddMySqlSink(
        this IFastIngestBuilder builder,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.MySqlConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
        });

        return builder;
    }

    /// <summary>
    /// Configures the default MySQL connection string for FastIngest on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddMySqlSink(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.Configure<FastIngestOptions>(options =>
        {
            options.MySqlConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
        });

        return services;
    }

    /// <summary>
    /// Configures SQLite bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddSqliteSink(this FastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder;
    }

    /// <summary>
    /// Configures SQLite bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddSqliteSink(this IFastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder;
    }

    /// <summary>
    /// Configures the default SQLite connection string for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddSqliteSink(
        this FastIngestBuilder builder,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.SqliteConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
        });

        return builder;
    }

    /// <summary>
    /// Configures the default SQLite connection string for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddSqliteSink(
        this IFastIngestBuilder builder,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.SqliteConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
        });

        return builder;
    }

    /// <summary>
    /// Configures the default SQLite connection string for FastIngest on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqliteSink(
        this IServiceCollection services,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.Configure<FastIngestOptions>(options =>
        {
            options.SqliteConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
        });

        return services;
    }

    /// <summary>
    /// Configures Azure Cosmos DB bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddCosmosDbSink(this FastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<CosmosClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;
            var connectionString = options.CosmosConnectionString ?? options.DefaultConnectionString;
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    "Azure Cosmos DB connection string is not configured. Configure CosmosConnectionString or DefaultConnectionString in FastIngestOptions, or register a CosmosClient in DI.");
            }

            return new CosmosClient(connectionString, new CosmosClientOptions { AllowBulkExecution = true });
        });

        return builder;
    }

    /// <summary>
    /// Configures Azure Cosmos DB bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddCosmosDbSink(this IFastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder is FastIngestBuilder concreteBuilder)
        {
            return concreteBuilder.AddCosmosDbSink();
        }

        builder.Services.TryAddSingleton<CosmosClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;
            var connectionString = options.CosmosConnectionString ?? options.DefaultConnectionString;
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new InvalidOperationException(
                    "Azure Cosmos DB connection string is not configured. Configure CosmosConnectionString or DefaultConnectionString in FastIngestOptions, or register a CosmosClient in DI.");
            }

            return new CosmosClient(connectionString, new CosmosClientOptions { AllowBulkExecution = true });
        });

        return builder;
    }

    /// <summary>
    /// Configures default Azure Cosmos DB connection settings for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddCosmosDbSink(
        this FastIngestBuilder builder,
        string connectionString,
        string? databaseName = null,
        string? containerName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.CosmosConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
            if (databaseName != null)
            {
                options.CosmosDatabaseName = databaseName;
            }
            if (containerName != null)
            {
                options.CosmosContainerName = containerName;
            }
        });

        AddCosmosDbSink(builder);
        return builder;
    }

    /// <summary>
    /// Configures default Azure Cosmos DB connection settings for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddCosmosDbSink(
        this IFastIngestBuilder builder,
        string connectionString,
        string? databaseName = null,
        string? containerName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.CosmosConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
            if (databaseName != null)
            {
                options.CosmosDatabaseName = databaseName;
            }
            if (containerName != null)
            {
                options.CosmosContainerName = containerName;
            }
        });

        return builder.AddCosmosDbSink();
    }

    /// <summary>
    /// Registers an existing <see cref="CosmosClient"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="cosmosClient">The configured Cosmos DB client.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddCosmosDbSink(
        this FastIngestBuilder builder,
        CosmosClient cosmosClient,
        string? databaseName = null,
        string? containerName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cosmosClient);

        builder.Services.AddSingleton<CosmosClient>(cosmosClient);

        if (databaseName != null || containerName != null)
        {
            builder.Services.Configure<FastIngestOptions>(options =>
            {
                if (databaseName != null) options.CosmosDatabaseName = databaseName;
                if (containerName != null) options.CosmosContainerName = containerName;
            });
        }

        AddCosmosDbSink(builder);
        return builder;
    }

    /// <summary>
    /// Registers an existing <see cref="CosmosClient"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="cosmosClient">The configured Cosmos DB client.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddCosmosDbSink(
        this IFastIngestBuilder builder,
        CosmosClient cosmosClient,
        string? databaseName = null,
        string? containerName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cosmosClient);

        builder.Services.AddSingleton<CosmosClient>(cosmosClient);

        if (databaseName != null || containerName != null)
        {
            builder.Services.Configure<FastIngestOptions>(options =>
            {
                if (databaseName != null) options.CosmosDatabaseName = databaseName;
                if (containerName != null) options.CosmosContainerName = containerName;
            });
        }

        return builder.AddCosmosDbSink();
    }

    /// <summary>
    /// Registers an existing <see cref="Container"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="container">The configured Cosmos DB container.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddCosmosDbSink(
        this FastIngestBuilder builder,
        Container container)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(container);

        builder.Services.AddSingleton<Container>(container);
        return builder;
    }

    /// <summary>
    /// Registers an existing <see cref="Container"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="container">The configured Cosmos DB container.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddCosmosDbSink(
        this IFastIngestBuilder builder,
        Container container)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(container);

        builder.Services.AddSingleton<Container>(container);
        return builder;
    }

    /// <summary>
    /// Configures default Azure Cosmos DB connection settings for FastIngest on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCosmosDbSink(
        this IServiceCollection services,
        string connectionString,
        string? databaseName = null,
        string? containerName = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.Configure<FastIngestOptions>(options =>
        {
            options.CosmosConnectionString = connectionString;
            options.DefaultConnectionString = connectionString;
            if (databaseName != null)
            {
                options.CosmosDatabaseName = databaseName;
            }
            if (containerName != null)
            {
                options.CosmosContainerName = containerName;
            }
        });

        services.TryAddSingleton<CosmosClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;
            var connStr = options.CosmosConnectionString ?? options.DefaultConnectionString;
            return new CosmosClient(connStr, new CosmosClientOptions { AllowBulkExecution = true });
        });

        return services;
    }

    /// <summary>
    /// Registers an existing <see cref="CosmosClient"/> instance on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="cosmosClient">The configured Cosmos DB client.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddCosmosDbSink(
        this IServiceCollection services,
        CosmosClient cosmosClient,
        string? databaseName = null,
        string? containerName = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(cosmosClient);

        services.AddSingleton<CosmosClient>(cosmosClient);

        if (databaseName != null || containerName != null)
        {
            services.Configure<FastIngestOptions>(options =>
            {
                if (databaseName != null) options.CosmosDatabaseName = databaseName;
                if (containerName != null) options.CosmosContainerName = containerName;
            });
        }

        return services;
    }

    /// <summary>
    /// Configures Elasticsearch bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddElasticsearchSink(this FastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.TryAddSingleton<ElasticsearchClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;
            var endpoint = options.ElasticsearchEndpoint;
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new InvalidOperationException(
                    "Elasticsearch endpoint is not configured. Configure ElasticsearchEndpoint in FastIngestOptions, or register an ElasticsearchClient in DI.");
            }

            var settings = new ElasticsearchClientSettings(new Uri(endpoint));
            if (!string.IsNullOrWhiteSpace(options.ElasticsearchApiKey))
            {
                settings.Authentication(new ApiKey(options.ElasticsearchApiKey));
            }
            return new ElasticsearchClient(settings);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures Elasticsearch bulk sink capabilities for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddElasticsearchSink(this IFastIngestBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder is FastIngestBuilder concreteBuilder)
        {
            return concreteBuilder.AddElasticsearchSink();
        }

        builder.Services.TryAddSingleton<ElasticsearchClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<FastIngestOptions>>().Value;
            var endpoint = options.ElasticsearchEndpoint;
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new InvalidOperationException(
                    "Elasticsearch endpoint is not configured. Configure ElasticsearchEndpoint in FastIngestOptions, or register an ElasticsearchClient in DI.");
            }

            var settings = new ElasticsearchClientSettings(new Uri(endpoint));
            if (!string.IsNullOrWhiteSpace(options.ElasticsearchApiKey))
            {
                settings.Authentication(new ApiKey(options.ElasticsearchApiKey));
            }
            return new ElasticsearchClient(settings);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Registers an existing <see cref="ElasticsearchClient"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="client">The configured Elasticsearch client.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddElasticsearchSink(
        this FastIngestBuilder builder,
        ElasticsearchClient client)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(client);

        builder.Services.AddSingleton<ElasticsearchClient>(client);
        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Registers an existing <see cref="ElasticsearchClient"/> instance for FastIngest.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="client">The configured Elasticsearch client.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddElasticsearchSink(
        this IFastIngestBuilder builder,
        ElasticsearchClient client)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(client);

        builder.Services.AddSingleton<ElasticsearchClient>(client);
        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures Elasticsearch sink connection parameters via <see cref="ElasticsearchClientSettings"/>.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="configureSettings">Action configuring Elasticsearch client settings.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddElasticsearchSink(
        this FastIngestBuilder builder,
        Action<ElasticsearchClientSettings> configureSettings)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureSettings);

        builder.Services.TryAddSingleton<ElasticsearchClient>(_ =>
        {
            var settings = new ElasticsearchClientSettings();
            configureSettings(settings);
            return new ElasticsearchClient(settings);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures Elasticsearch sink connection parameters via <see cref="ElasticsearchClientSettings"/>.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="configureSettings">Action configuring Elasticsearch client settings.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddElasticsearchSink(
        this IFastIngestBuilder builder,
        Action<ElasticsearchClientSettings> configureSettings)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureSettings);

        builder.Services.TryAddSingleton<ElasticsearchClient>(_ =>
        {
            var settings = new ElasticsearchClientSettings();
            configureSettings(settings);
            return new ElasticsearchClient(settings);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI and optional API key.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddElasticsearchSink(
        this FastIngestBuilder builder,
        Uri endpoint,
        string? apiKey = null,
        string? defaultIndex = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(endpoint);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.ElasticsearchEndpoint = endpoint.OriginalString;
            options.ElasticsearchApiKey = apiKey;
            if (defaultIndex != null)
            {
                options.ElasticsearchDefaultIndex = defaultIndex;
            }
        });

        builder.Services.TryAddSingleton<ElasticsearchClient>(_ =>
        {
            var settings = new ElasticsearchClientSettings(endpoint);
            if (!string.IsNullOrWhiteSpace(apiKey))
            {
                settings.Authentication(new ApiKey(apiKey));
            }
            return new ElasticsearchClient(settings);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI and optional API key.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddElasticsearchSink(
        this IFastIngestBuilder builder,
        Uri endpoint,
        string? apiKey = null,
        string? defaultIndex = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(endpoint);

        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.ElasticsearchEndpoint = endpoint.OriginalString;
            options.ElasticsearchApiKey = apiKey;
            if (defaultIndex != null)
            {
                options.ElasticsearchDefaultIndex = defaultIndex;
            }
        });

        builder.Services.TryAddSingleton<ElasticsearchClient>(_ =>
        {
            var settings = new ElasticsearchClientSettings(endpoint);
            if (!string.IsNullOrWhiteSpace(apiKey))
            {
                settings.Authentication(new ApiKey(apiKey));
            }
            return new ElasticsearchClient(settings);
        });

        builder.Services.TryAddTransient(typeof(IIngestionSink<>), typeof(ElasticsearchIngestionSink<>));
        return builder;
    }

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI string and optional API key.
    /// </summary>
    /// <param name="builder">The FastIngest builder instance.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI string.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static FastIngestBuilder AddElasticsearchSink(
        this FastIngestBuilder builder,
        string endpoint,
        string? apiKey = null,
        string? defaultIndex = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);

        AddElasticsearchSink(builder, new Uri(endpoint), apiKey, defaultIndex);
        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.ElasticsearchEndpoint = endpoint;
        });
        return builder;
    }

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI string and optional API key.
    /// </summary>
    /// <param name="builder">The FastIngest builder interface instance.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI string.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The builder instance for fluent chaining.</returns>
    public static IFastIngestBuilder AddElasticsearchSink(
        this IFastIngestBuilder builder,
        string endpoint,
        string? apiKey = null,
        string? defaultIndex = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);

        builder.AddElasticsearchSink(new Uri(endpoint), apiKey, defaultIndex);
        builder.Services.Configure<FastIngestOptions>(options =>
        {
            options.ElasticsearchEndpoint = endpoint;
        });
        return builder;
    }

    /// <summary>
    /// Registers an existing <see cref="ElasticsearchClient"/> instance on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="client">The configured Elasticsearch client.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddElasticsearchSink(
        this IServiceCollection services,
        ElasticsearchClient client)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(client);

        services.AddSingleton<ElasticsearchClient>(client);
        return services;
    }

    /// <summary>
    /// Configures Elasticsearch sink connection parameters via <see cref="ElasticsearchClientSettings"/> on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configureSettings">Action configuring Elasticsearch client settings.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddElasticsearchSink(
        this IServiceCollection services,
        Action<ElasticsearchClientSettings> configureSettings)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureSettings);

        services.TryAddSingleton<ElasticsearchClient>(_ =>
        {
            var settings = new ElasticsearchClientSettings();
            configureSettings(settings);
            return new ElasticsearchClient(settings);
        });

        return services;
    }

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI and optional API key on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddElasticsearchSink(
        this IServiceCollection services,
        Uri endpoint,
        string? apiKey = null,
        string? defaultIndex = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(endpoint);

        services.Configure<FastIngestOptions>(options =>
        {
            options.ElasticsearchEndpoint = endpoint.OriginalString;
            options.ElasticsearchApiKey = apiKey;
            if (defaultIndex != null)
            {
                options.ElasticsearchDefaultIndex = defaultIndex;
            }
        });

        services.TryAddSingleton<ElasticsearchClient>(_ =>
        {
            var settings = new ElasticsearchClientSettings(endpoint);
            if (!string.IsNullOrWhiteSpace(apiKey))
            {
                settings.Authentication(new ApiKey(apiKey));
            }
            return new ElasticsearchClient(settings);
        });

        return services;
    }

    /// <summary>
    /// Configures Elasticsearch connection settings with an endpoint URI string and optional API key on <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="endpoint">The Elasticsearch server endpoint URI string.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddElasticsearchSink(
        this IServiceCollection services,
        string endpoint,
        string? apiKey = null,
        string? defaultIndex = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);

        services.AddElasticsearchSink(new Uri(endpoint), apiKey, defaultIndex);
        services.Configure<FastIngestOptions>(options =>
        {
            options.ElasticsearchEndpoint = endpoint;
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
