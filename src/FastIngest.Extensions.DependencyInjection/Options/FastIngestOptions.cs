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
    /// Gets or sets the default bounded channel capacity (batches in flight) between reader and sink. Defaults to 2.
    /// </summary>
    public int ChannelCapacity { get; set; } = 2;

    /// <summary>
    /// Gets or sets the default bounded channel capacity (batches in flight) between reader and sink. Defaults to 2.
    /// </summary>
    public int DefaultChannelCapacity
    {
        get => ChannelCapacity;
        set => ChannelCapacity = value;
    }

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

    /// <summary>
    /// Gets or sets the default MySQL connection string.
    /// </summary>
    public string? MySqlConnectionString { get; set; }

    /// <summary>
    /// Configures the default MySQL connection string.
    /// </summary>
    /// <param name="connectionString">The MySQL connection string.</param>
    /// <returns>The options instance for fluent chaining.</returns>
    public FastIngestOptions AddMySqlSink(string connectionString)
    {
        MySqlConnectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new ArgumentNullException(nameof(connectionString))
            : connectionString;
        DefaultConnectionString = MySqlConnectionString;
        return this;
    }

    /// <summary>
    /// Gets or sets the default SQLite connection string.
    /// </summary>
    public string? SqliteConnectionString { get; set; }

    /// <summary>
    /// Configures the default SQLite connection string.
    /// </summary>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <returns>The options instance for fluent chaining.</returns>
    public FastIngestOptions AddSqliteSink(string connectionString)
    {
        SqliteConnectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new ArgumentNullException(nameof(connectionString))
            : connectionString;
        DefaultConnectionString = SqliteConnectionString;
        return this;
    }

    /// <summary>
    /// Gets or sets the default Azure Cosmos DB connection string.
    /// </summary>
    public string? CosmosConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the default Azure Cosmos DB database name.
    /// </summary>
    public string? CosmosDatabaseName { get; set; }

    /// <summary>
    /// Gets or sets the default Azure Cosmos DB container name.
    /// </summary>
    public string? CosmosContainerName { get; set; }

    /// <summary>
    /// Configures the default Azure Cosmos DB connection settings.
    /// </summary>
    /// <param name="connectionString">The Azure Cosmos DB connection string.</param>
    /// <param name="databaseName">Optional default database name.</param>
    /// <param name="containerName">Optional default container name.</param>
    /// <returns>The options instance for fluent chaining.</returns>
    public FastIngestOptions AddCosmosDbSink(
        string connectionString,
        string? databaseName = null,
        string? containerName = null)
    {
        CosmosConnectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new ArgumentNullException(nameof(connectionString))
            : connectionString;
        CosmosDatabaseName = databaseName;
        CosmosContainerName = containerName;
        return this;
    }

    /// <summary>
    /// Gets or sets the default Elasticsearch endpoint URI string.
    /// </summary>
    public string? ElasticsearchEndpoint { get; set; }

    /// <summary>
    /// Gets or sets the default Elasticsearch API key.
    /// </summary>
    public string? ElasticsearchApiKey { get; set; }

    /// <summary>
    /// Gets or sets the default Elasticsearch index name.
    /// </summary>
    public string? ElasticsearchDefaultIndex { get; set; }

    /// <summary>
    /// Configures the default Elasticsearch connection settings.
    /// </summary>
    /// <param name="endpoint">The Elasticsearch server endpoint URI.</param>
    /// <param name="apiKey">Optional API key for authentication.</param>
    /// <param name="defaultIndex">Optional default index name.</param>
    /// <returns>The options instance for fluent chaining.</returns>
    public FastIngestOptions AddElasticsearchSink(
        string endpoint,
        string? apiKey = null,
        string? defaultIndex = null)
    {
        ElasticsearchEndpoint = string.IsNullOrWhiteSpace(endpoint)
            ? throw new ArgumentNullException(nameof(endpoint))
            : endpoint;
        ElasticsearchApiKey = apiKey;
        ElasticsearchDefaultIndex = defaultIndex;
        return this;
    }
}
