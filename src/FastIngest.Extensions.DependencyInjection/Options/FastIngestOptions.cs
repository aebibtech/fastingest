using FastIngest.Core.Common;

namespace FastIngest.Extensions.DependencyInjection.Options;

/// <summary>
/// Configuration options for FastIngest dependency injection and execution engine.
/// </summary>
public class FastIngestOptions
{
    /// <summary>
    /// Gets or sets the default PostgreSQL connection string used when not specified in the profile.
    /// </summary>
    public string? DefaultConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the default batch size when not specified in the profile. Defaults to 5,000.
    /// </summary>
    public int DefaultBatchSize { get; set; } = 5000;

    /// <summary>
    /// Gets or sets the default error handling strategy. Defaults to <see cref="ErrorStrategy.FailFast"/>.
    /// </summary>
    public ErrorStrategy DefaultErrorStrategy { get; set; } = ErrorStrategy.FailFast;

    /// <summary>
    /// Configures the default PostgreSQL connection string.
    /// </summary>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The options instance for fluent chaining.</returns>
    public FastIngestOptions AddPostgreSqlSink(string connectionString)
    {
        DefaultConnectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new ArgumentNullException(nameof(connectionString))
            : connectionString;
        return this;
    }

    /// <summary>
    /// Configures the default Microsoft SQL Server connection string.
    /// </summary>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The options instance for fluent chaining.</returns>
    public FastIngestOptions AddSqlServerSink(string connectionString)
    {
        DefaultConnectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new ArgumentNullException(nameof(connectionString))
            : connectionString;
        return this;
    }

    /// <summary>
    /// Gets or sets the default MongoDB database name.
    /// </summary>
    public string? MongoDatabaseName { get; set; }

    /// <summary>
    /// Gets or sets the default MongoDB connection string.
    /// </summary>
    public string? MongoConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the default MongoDB collection name when not specified in a profile.
    /// </summary>
    public string? MongoCollectionName { get; set; }

    /// <summary>
    /// Configures the default MongoDB connection settings.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <returns>The options instance for fluent chaining.</returns>
    public FastIngestOptions AddMongoDbSink(string connectionString, string? databaseName = null)
    {
        MongoConnectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new ArgumentNullException(nameof(connectionString))
            : connectionString;
        DefaultConnectionString = MongoConnectionString;
        MongoDatabaseName = databaseName;
        return this;
    }
}
